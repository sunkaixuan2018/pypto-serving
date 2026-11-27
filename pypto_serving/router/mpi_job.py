# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the License.
# -----------------------------------------------------------------------------------------------------------

"""Own one Simpler MPI supervisor and its authenticated rank-zero session."""

import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time

from pypto_serving.pd.mpi_control import HOST, receive_rank_frame, send_rank_frame

logger = logging.getLogger(__name__)
_ROUTER_TOKEN_ENV = "SIMPLER_MPI_DIRECT_ROUTER_TOKEN"
_STATES = {"starting", "ready", "draining", "failed", "disconnected", "exited"}


class MPIJobError(RuntimeError):
    """An MPI deployment failed or its cleanup could not be confirmed."""


def launch_supervisor(config, router_arguments):
    """Launch Router and Supervisor as independent children of the deployment command."""
    from pypto_serving.pd.config import Config

    cfg = config.mpi_pd
    if cfg is None or cfg.owner != "supervisor":
        raise ValueError("--supervise-mpi requires mpi_pd.owner=supervisor")
    if not os.environ.get(_ROUTER_TOKEN_ENV):
        raise ValueError(f"{_ROUTER_TOKEN_ENV} is required for a Supervisor-owned deployment")
    router_command = [sys.executable, "-m", "pypto_serving.router.app", *router_arguments]
    manifest = cfg.manifest(config, Config.from_router(config))
    with tempfile.TemporaryDirectory(prefix="serving-mpi-") as directory:
        path = Path(directory) / "deployment.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        command = [cfg.supervisor_python, "-m", "simpler.mpi_direct_supervisor",
                   "--service-config", str(path), "--mpirun", cfg.mpirun, "--launcher-family", cfg.launcher_family]
        if cfg.rank_python is not None:
            command.extend(("--python", cfg.rank_python))
        options = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt"
                   else {"start_new_session": True})
        stopping = False
        def stop(_signum, _frame):
            nonlocal stopping
            stopping = True

        def signal_stop(process):
            if process.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)

        handlers = {}
        try:
            for signum in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGHUP", None)):
                if signum is not None:
                    handlers[signum] = signal.signal(signum, stop)
            router = subprocess.Popen(router_command, **options)
            try:
                supervisor = subprocess.Popen(command, **options)
                print(f"MPI deployment processes: Router PID {router.pid}, Supervisor PID {supervisor.pid}",
                      flush=True)
                while not stopping:
                    if supervisor.poll() is not None and router.poll() is not None:
                        break
                    time.sleep(0.1)
                signal_stop(supervisor)
                status = supervisor.wait()
            finally:
                signal_stop(router)
                try:
                    router.wait(timeout=cfg.drain_timeout_s + 15)
                except subprocess.TimeoutExpired:
                    router.kill()
                    router.wait()
            return status or router.returncode
        finally:
            for signum, handler in handlers.items():
                signal.signal(signum, handler)


class MPIJob:
    def __init__(self, deployment, manifest, withdraw, probe):
        self.deployment = deployment
        self.manifest = manifest
        self.withdraw = withdraw
        self.probe = probe
        self.instances = {s.replica.name: (rank, s.replica.role)
                          for rank, s in enumerate(deployment.services, 1)}
        self.states = {}
        self.ready = {}
        self.descriptors = set()
        self.exits = {}
        self.error = None
        self.startup_error = None
        self.connected = False
        self.stopping = False
        self.closed = False
        self.log_path = None
        self._writer = None
        self._reply = None
        self._shutdown_id = None
        self._authentication = asyncio.Event()
        self._stop_task = None
        self._process = None

    async def start(self):
        cfg = self.deployment
        cfg.log_dir.mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix="job-", dir=cfg.log_dir)).resolve()
        manifest_path = directory / "service-config.json"
        manifest_path.write_text(json.dumps(self.manifest), encoding="utf-8")
        self.log_path = directory / "supervisor.log"
        command = [cfg.supervisor_python, "-m", "simpler.mpi_direct_supervisor",
                   "--service-config", str(manifest_path), "--mpirun", cfg.mpirun,
                   "--launcher-family", cfg.launcher_family]
        if cfg.rank_python is not None:
            command.extend(("--python", cfg.rank_python))
        token = secrets.token_urlsafe(32)
        try:
            with self.log_path.open("wb") as log:
                self._process = await asyncio.create_subprocess_exec(
                    *command, env={**os.environ, _ROUTER_TOKEN_ENV: token}, stdout=log,
                    stderr=asyncio.subprocess.STDOUT, start_new_session=os.name != "nt",
                )
        except OSError as exc:
            manifest_path.unlink(missing_ok=True)
            raise MPIJobError(f"failed to start MPI supervisor for {self._describe(self.instances)}: {exc}; "
                              f"see {self.log_path}") from exc
        logger.info("MPI supervisor PID %s started for %s; log %s", self._process.pid,
                    self._describe(self.instances), self.log_path)
        self._reader = asyncio.create_task(self._read(token))
        self._waiter = asyncio.create_task(self._process.wait())
        self._monitor = asyncio.create_task(self._watch())
        self._startup = asyncio.create_task(self._startup_deadline())

    def _describe(self, names):
        return ", ".join(f"{name}={self.states.get(name, 'pending')}"
                         + (f"({self.exits[name]})" if name in self.exits else "") for name in names)

    def status(self):
        return {
            "connected": self.connected,
            "stopping": self.stopping,
            "closed": self.closed,
            "error": self.error,
            "startup_error": self.startup_error,
            "instances": {
                name: {"rank": rank, "role": role, "state": self.states.get(name, "pending"),
                       "descriptor_verified": name in self.descriptors,
                       "returncode": self.exits.get(name)}
                for name, (rank, role) in self.instances.items()
            },
        }

    def expects_descriptor(self, name):
        return self.connected and not self.stopping and not self.error and name in self.ready

    def matches_descriptor(self, node):
        return (self.expects_descriptor(node.node_id)
                and self.ready[node.node_id] == (node.boot, node.registry))

    def confirm_descriptor(self, name):
        self.descriptors.add(name)
        if self.instances.keys() <= self.descriptors:
            self.startup_error = None

    def forget_descriptor(self, name):
        self.descriptors.discard(name)

    def allows_pair(self, _prefill, _decode):
        return True

    def _withdraw(self, name):
        self.ready.pop(name, None)
        self.descriptors.discard(name)
        self.withdraw(name)

    def _fail(self, message):
        if self.error is None:
            self.error = f"{message}; instances: {self._describe(self.instances)}"
            logger.error("%s; supervisor log: %s", self.error, self.log_path)
        for name in self.instances:
            self._withdraw(name)

    async def _startup_deadline(self):
        await asyncio.sleep(self.deployment.startup_timeout_s)
        pending = self.instances.keys() - self.descriptors
        if pending and not self.stopping:
            self.startup_error = f"MPI startup timed out waiting for {self._describe(sorted(pending))}"
            logger.error("%s; descriptor must match the rank-0 readiness identity; see %s",
                         self.startup_error, self.log_path)

    async def _connect_once(self, token):
        reader, writer = await asyncio.open_connection(HOST, self.deployment.router_port)
        try:
            await send_rank_frame(writer, {"type": "hello", "token": token})
            if await receive_rank_frame(reader) != {"type": "hello", "rank": 0}:
                raise MPIJobError("unexpected rank-0 authentication reply")
            return reader, writer
        except BaseException:
            writer.close()
            raise

    async def _authenticate(self, token):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.deployment.auth_timeout_s
        while self._process.returncode is None and (remaining := deadline - loop.time()) > 0:
            try:
                # Rank 0 can bind before MPI initialization starts its reader.
                # Closing a queued hello early can make it accept a dead peer.
                return await asyncio.wait_for(self._connect_once(token), remaining)
            except (OSError, EOFError, asyncio.TimeoutError):
                await asyncio.sleep(min(0.1, max(0, deadline - loop.time())))
        if self._process.returncode is not None:
            raise MPIJobError(f"MPI supervisor exited before rank-0 authentication (code {self._process.returncode})")
        raise MPIJobError("timed out authenticating to MPI rank 0")

    def _event(self, frame):
        if frame.get("type") == "response":
            if self._shutdown_id is None or frame.get("id") != self._shutdown_id or self._reply is not None:
                raise MPIJobError("unexpected MPI rank-0 response")
            self._reply = frame
            return

        name, state = frame.get("instance_id"), frame.get("state")
        rank, role, payload = frame.get("rank"), frame.get("role"), frame.get("payload")
        if (frame.get("type") != "status" or not isinstance(name, str)
                or type(rank) is not int or self.instances.get(name) != (rank, role)
                or not isinstance(state, str) or state not in _STATES
                or not isinstance(payload, dict) or name in self.exits):
            raise MPIJobError("invalid MPI instance status from rank 0")
        identity = payload.get("boot"), payload.get("registry_digest")
        if state == "ready" and not all(isinstance(part, str) and part for part in identity):
            raise MPIJobError(f"missing readiness identity for {name}")
        if state == "exited" and type(payload.get("returncode")) is not int:
            raise MPIJobError(f"missing exit code for {name}")

        self.states[name] = state
        if state != "ready" or self.ready.get(name) != identity:
            self._withdraw(name)
        if state == "ready" and not self.stopping and not self.error:
            self.ready[name] = identity
            self.probe()
        if state == "exited":
            self.exits[name] = payload["returncode"]
        if state in ("failed", "exited") and not self.stopping:
            self._fail(f"MPI service {name} {state}")
        logger.info("MPI service %s (%s rank %s) is %s%s", name, role, rank, state,
                    f" (code {self.exits[name]})" if state == "exited" else "")

    async def _read(self, token):
        try:
            reader, self._writer = await self._authenticate(token)
            self.connected = True
            self._authentication.set()
            while True:
                self._event(await receive_rank_frame(reader))
        except EOFError:
            if not self.stopping:
                self._fail("MPI rank-0 control channel disconnected")
        except (OSError, ValueError, MPIJobError) as exc:
            self._fail(str(exc))
        finally:
            self.connected = False
            self._authentication.set()
            if self._writer is not None:
                self._writer.close()
            for name in self.instances:
                self._withdraw(name)

    async def _watch(self):
        done, _ = await asyncio.wait((self._reader, self._waiter), return_when=asyncio.FIRST_COMPLETED)
        if not self.stopping:
            if self._waiter in done:
                self._fail(f"MPI supervisor exited (code {self._process.returncode})")
            with contextlib.suppress(MPIJobError):
                await self.stop()

    async def stop(self):
        if self._process is None:
            return
        if self._stop_task is None:
            self.stopping = True
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)

    async def _finish(self):
        # Wait for rank 0 even if shutdown arrives while the supervisor starts.
        await self._authentication.wait()
        if self.connected and not self._waiter.done():
            self._shutdown_id = secrets.token_hex(16)
            try:
                await send_rank_frame(self._writer, {"type": "shutdown", "id": self._shutdown_id})
            except (ConnectionError, OSError):
                self._fail("could not send shutdown to rank 0")
        await asyncio.gather(asyncio.shield(self._reader), asyncio.shield(self._waiter))

    async def _terminate(self):
        # Closing the control socket gives rank 0 a chance to finish its own
        # cleanup. A forced local exit cannot prove that remote children stopped.
        self._reader.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._reader
        for action in (None, self._process.terminate, self._process.kill):
            if self._waiter.done():
                return
            if action is not None:
                with contextlib.suppress(ProcessLookupError):
                    action()
            try:
                await asyncio.wait_for(asyncio.shield(self._waiter), 5)
                return
            except asyncio.TimeoutError:
                pass

    async def _stop(self):
        self._startup.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._startup
        try:
            await asyncio.wait_for(self._finish(), self.deployment.shutdown_timeout_s)
        except asyncio.TimeoutError as exc:
            await self._terminate()
            self._fail("MPI shutdown timed out")
            unknown = self.instances.keys() - self.exits.keys()
            detail = f"cleanup unconfirmed for {self._describe(sorted(unknown))}" if unknown else (
                "all service exits were reported; supervisor shutdown was not confirmed")
            logger.error("MPI shutdown timed out: %s; see %s", detail, self.log_path)
            raise MPIJobError(f"MPI shutdown timed out: {detail}; see {self.log_path}") from exc
        finally:
            self.closed = True
            with contextlib.suppress(OSError):
                (self.log_path.parent / "service-config.json").unlink(missing_ok=True)

        missing = self.instances.keys() - self.exits.keys()
        if missing:
            detail = f"MPI cleanup unconfirmed for {self._describe(sorted(missing))}"
        elif (self.error or self._process.returncode != 0 or any(self.exits.values())
                or self._reply is None or self._reply.get("ok") is not True):
            detail = f"MPI deployment ended with errors (supervisor code {self._process.returncode})"
        else:
            order = tuple(self.instances[name][1] for name in self.exits)
            if order == ("prefill",) * order.count("prefill") + ("decode",) * order.count("decode"):
                logger.info("MPI shutdown confirmed for %s", self._describe(self.exits))
                return
            detail = "MPI shutdown did not report all Prefill exits before Decode"
        self._fail(detail)
        logger.error("%s; instances: %s; supervisor log: %s", detail, self._describe(self.instances), self.log_path)
        raise MPIJobError(f"{detail}; instances: {self._describe(self.instances)}; see {self.log_path}")
