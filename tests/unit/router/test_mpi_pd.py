# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the License.
# -----------------------------------------------------------------------------------------------------------

"""Router-owned MPI Direct deployments and their readiness boundaries."""

import asyncio
import socket

import httpx
import msgspec
import pytest

from pypto_serving.pd.config import Config
from pypto_serving.pd.mpi_control import receive_rank_frame, send_rank_frame
from pypto_serving.pd.protocol import Component, Contract, Node, Topology
from pypto_serving.router.app import ServingRouter
from pypto_serving.router.config import parse_router_config
from pypto_serving.router.mpi_config import PD_CONFIG_ARG, PD_CONFIG_ENV
from pypto_serving.router.mpi_job import MPIJobError
from starlette.requests import Request


def document(tmp_path, port=22000):
    return {
        "replicas": [
            {"name": "p", "role": "prefill", "host": "127.0.0.1", "port": 22001},
            {"name": "d", "role": "decode", "host": "127.0.0.1", "port": 22002},
            {"name": "late", "role": "decode", "host": "127.0.0.1", "port": 22003},
        ],
        "pd": {"provider": "hixl", "handoff_timeout": 1, "request_timeout": 3},
        "observability": {"enabled": False},
        "mpi_pd": {
            "router_port": port, "auth_timeout_s": 2, "startup_timeout_s": 4, "shutdown_timeout_s": 3,
            "log_dir": str(tmp_path),
            "services": {name: {"devices": [i], "argv": ["pypto-serving", "--model", "/model"]}
                         for i, name in enumerate(("p", "d", "late"))},
        },
    }


def test_rank_bootstrap_has_the_same_pd_configuration(tmp_path, monkeypatch):
    cfg = parse_router_config(document(tmp_path))
    model = Config.from_router(cfg)
    services = cfg.mpi_pd.manifest(cfg, model)["services"]
    for service in services:
        assert service["argv"][-2:] == ["--pd-config", PD_CONFIG_ARG]
        monkeypatch.setenv("SIMPLER_MPI_DIRECT_INSTANCE_ID", service["id"])
        monkeypatch.setenv(PD_CONFIG_ENV, service["env"][PD_CONFIG_ENV])
        assert Config.load(PD_CONFIG_ARG) == model
    assert services[1]["bind_port"] == 32002


@pytest.mark.parametrize("edit, message", [
    (lambda d: d["mpi_pd"]["services"].pop("d"), "one launch entry per replica ID"),
    (lambda d: d["mpi_pd"]["services"]["d"].update(devices=[0]), "overlaps another MPI device group"),
    (lambda d: d["replicas"][1].update(control_port=22000), "conflicts"),
    (lambda d: d["replicas"][1].update(host="10.0.0.2"), "reachable addresses"),
    (lambda d: d["mpi_pd"]["services"]["d"]["argv"].append("--pd-config=file"), "leave host, port"),
    (lambda d: d["mpi_pd"].update(auth_timeout_s=9), "cannot exceed"),
])
def test_rejects_conflicting_deployments(tmp_path, edit, message):
    deployment = document(tmp_path)
    edit(deployment)
    with pytest.raises(ValueError, match=message):
        parse_router_config(deployment)


class Supervisor:
    def __init__(self):
        self.done = asyncio.Event()
        self.handshake = asyncio.Event()
        self.returncode = None
        self.pid = 42
        self.shutdown = None

    async def spawn(self, *argv, env, **_):
        assert argv[1:3] == ("-m", "simpler.mpi_direct_supervisor")
        self.token = env["SIMPLER_MPI_DIRECT_ROUTER_TOKEN"]
        return self

    async def wait(self):
        await self.done.wait()
        return self.returncode

    async def accept(self, reader, writer):
        self.writer = writer
        try:
            assert await receive_rank_frame(reader) == {"type": "hello", "token": self.token}
            await send_rank_frame(writer, {"type": "hello", "rank": 0})
            self.handshake.set()
            self.shutdown = await receive_rank_frame(reader)
            assert self.shutdown["type"] == "shutdown"
            for name in ("p", "d", "late"):
                await self.report(name, "exited", {"returncode": 0})
            await send_rank_frame(writer, {"type": "response", "id": self.shutdown["id"], "ok": True})
            self.returncode = 0
        except (EOFError, OSError):
            self.returncode = 1
        finally:
            writer.close()
            self.done.set()

    async def report(self, name, state, payload):
        await send_rank_frame(self.writer, {
            "type": "status", "instance_id": name, "role": "prefill" if name == "p" else "decode",
            "rank": ("p", "d", "late").index(name) + 1, "state": state, "payload": payload,
        })


async def eventually(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(wait(), 2)


def test_router_gates_stale_descriptors_and_stops_the_job(tmp_path, monkeypatch):
    async def run():
        supervisor = Supervisor()
        monkeypatch.setattr(asyncio, "create_subprocess_exec", supervisor.spawn)
        server = await asyncio.start_server(supervisor.accept, "127.0.0.1", 0, family=socket.AF_INET)
        cfg = parse_router_config(document(tmp_path, server.sockets[0].getsockname()[1]),
                                  health_interval_seconds=0.03)
        contract = Contract("model", "model", "layout", (Component("kv", "kv", "kv"),))
        nodes = {s.name: Node(s.name, s.role, "boot", contract, "registry", Topology.for_tp(1, 1), "hixl")
                 for s in cfg.replicas}
        current = {**nodes, "d": msgspec.structs.replace(nodes["d"], boot="old-boot")}

        def reply(request):
            spec = next(s for s in cfg.replicas if s.port == request.url.port)
            return httpx.Response(200, json=msgspec.to_builtins(current[spec.name]))

        router = ServingRouter(cfg, client_factory=lambda _: httpx.AsyncClient(
            transport=httpx.MockTransport(reply), timeout=1))
        async with server, router.app.router.lifespan_context(router.app):
            await asyncio.wait_for(supervisor.handshake.wait(), 2)
            assert not router.proxy.pairs()
            for name in ("p", "d"):
                await supervisor.report(name, "ready", {"boot": "boot", "registry_digest": "registry"})
            await eventually(lambda: router.registry.state("p").ready)
            assert not router.registry.state("d").ready
            assert not router.proxy.pairs()

            current["d"] = msgspec.structs.replace(nodes["d"], registry="old-registry")
            await router.health.probe_once()
            assert not router.proxy.pairs()

            current["d"] = nodes["d"]
            await eventually(lambda: len(router.proxy.pairs()) == 1)
            assert not router.registry.state("late").ready
            await supervisor.report("d", "disconnected", {})
            await eventually(lambda: not router.proxy.pairs())

            await supervisor.report("late", "ready", {"boot": "boot", "registry_digest": "registry"})
            await eventually(lambda: len(router.proxy.pairs()) == 1)
            assert router.proxy.pairs()[0][1].node_id == "late"

        assert supervisor.shutdown is not None
        assert router.proxy.closing
        assert router.mpi_job.exits == {"p": 0, "d": 0, "late": 0}
        assert router.mpi_job.log_path.is_file()
        assert not (router.mpi_job.log_path.parent / "service-config.json").exists()
    asyncio.run(run())


def test_disconnected_rank_zero_marks_cleanup_unconfirmed(tmp_path, monkeypatch):
    async def run():
        supervisor = Supervisor()

        async def cut_off(reader, writer):
            await receive_rank_frame(reader)
            await send_rank_frame(writer, {"type": "hello", "rank": 0})
            writer.close()
            await writer.wait_closed()
            supervisor.returncode = 1
            supervisor.done.set()

        monkeypatch.setattr(asyncio, "create_subprocess_exec", supervisor.spawn)
        server = await asyncio.start_server(cut_off, "127.0.0.1", 0)
        cfg = parse_router_config(document(tmp_path, server.sockets[0].getsockname()[1]))
        router = ServingRouter(cfg, client_factory=lambda _: httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(503))))
        async with server:
            with pytest.raises(MPIJobError, match="cleanup unconfirmed.*d=.*late=.*p="):
                async with router.app.router.lifespan_context(router.app):
                    await eventually(lambda: router.mpi_job.closed)
                    assert not router.mpi_job.connected
                    assert not router.proxy.pairs()
    asyncio.run(run())


def test_supervisor_subscription_drains_a_stream_after_prefill_has_returned(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setenv("SIMPLER_MPI_DIRECT_ROUTER_TOKEN", "fixture-token")
        command = asyncio.Queue()
        received = asyncio.Queue()
        release = asyncio.Event()

        async def rank_zero(reader, writer):
            try:
                assert await receive_rank_frame(reader) == {"type": "hello", "token": "fixture-token"}
                await send_rank_frame(writer, {"type": "hello", "rank": 0})
                while True:
                    frame = await command.get()
                    if frame is None:
                        break
                    await send_rank_frame(writer, frame)
                    if frame["type"] == "drain":
                        await received.put(await receive_rank_frame(reader))
            finally:
                writer.close()
                await writer.wait_closed()

        listener = await asyncio.start_server(rank_zero, "127.0.0.1", 0)
        data = document(tmp_path, listener.sockets[0].getsockname()[1])
        data["mpi_pd"]["owner"] = "supervisor"
        data["pd"]["handoff_timeout"] = 1
        cfg = parse_router_config(data, health_interval_seconds=0.03)
        contract = Contract("model", "model", "layout", (Component("kv", "kv", "kv"),))
        nodes = {s.name: Node(s.name, s.role, "boot", contract, "registry", Topology.for_tp(1, 1), "hixl")
                 for s in cfg.replicas}
        instances = [{"id": s.name, "rank": rank, "role": s.role, "http_url": s.base_url,
                      "boot": "boot", "registry_digest": "registry"}
                     for rank, s in enumerate(cfg.replicas, 1)]

        class Delayed(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'data: {"choices":[{"text":"answer"}]}\n\n'
                await release.wait()
                yield b"data: [DONE]\n\n"

        async def reply(http):
            spec = next(s for s in cfg.replicas if s.port == http.url.port)
            if http.url.path.endswith("descriptor"):
                return httpx.Response(200, json=msgspec.to_builtins(nodes[spec.name]))
            if spec.role == "prefill":
                binding = msgspec.json.decode(http.headers["x-pd-context"])["binding"]
                return httpx.Response(200, json={"request": binding["request"], "status": "ready"})
            if msgspec.json.decode(http.content).get("stream"):
                return httpx.Response(200, stream=Delayed())
            return httpx.Response(200, json={"choices": [{"text": spec.name}]})

        def req(stream):
            async def body():
                return {"type": "http.request", "body": msgspec.json.encode({"stream": stream})}
            return Request({"type": "http", "method": "POST", "headers": []}, body)

        def snapshot(first, second, *, draining=False):
            return {"type": "snapshot", "draining": draining, "instances": instances,
                    "pairs": [{"prefill": "p", "decode": name, "state": state}
                              for name, state in (("d", first), ("late", second))]}

        router = ServingRouter(cfg, client_factory=lambda _: httpx.AsyncClient(
            transport=httpx.MockTransport(reply), timeout=2))
        async with listener, router.app.router.lifespan_context(router.app):
            await command.put(snapshot("preparing", "on_demand"))
            await eventually(lambda: len(router.proxy.pairs()) == 1)
            body = await router.proxy.forward(req(False), "/v1/completions")
            assert msgspec.json.decode(body.body)["choices"][0]["text"] == "late"

            await command.put(None)
            await eventually(lambda: not router.mpi_job.connected and not router.proxy.pairs())
            await command.put(snapshot("preparing", "on_demand"))
            await eventually(lambda: len(router.proxy.pairs()) == 1)

            instances[1] = {**instances[1], "boot": "new-boot"}
            await command.put(snapshot("prebuilt", "failed"))
            await eventually(lambda: not router.proxy.pairs())
            nodes["d"] = msgspec.structs.replace(nodes["d"], boot="new-boot")
            await command.put(snapshot("prebuilt", "failed"))
            await eventually(lambda: [d.node_id for _, d in router.proxy.pairs()] == ["d"])
            response = await router.proxy.forward(req(True), "/v1/completions")
            assert "answer" in await anext(response.body_iterator)
            await eventually(lambda: router.registry.state("p").outstanding == 0)
            assert router.registry.state("d").outstanding == 1
            await command.put(snapshot("retired", "retired", draining=True))
            await command.put({"type": "drain", "id": "stop", "timeout_s": 2})
            await eventually(lambda: not router.proxy.pairs())
            rejection = await router.proxy.forward(req(False), "/v1/completions")
            assert rejection.status_code == 503 and received.empty()
            release.set()
            assert [item async for item in response.body_iterator] == ["data: [DONE]\n\n"]
            assert await asyncio.wait_for(received.get(), 2) == {
                "type": "drain_result", "id": "stop", "ok": True, "remaining": 0,
            }
            await command.put(None)
            await eventually(lambda: not router.mpi_job.connected)
    asyncio.run(run())
