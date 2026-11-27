# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the License.
# -----------------------------------------------------------------------------------------------------------
"""Subscribe to rank zero from an independent Serving Router."""

import asyncio
from contextlib import suppress
import logging
import os
import secrets

from pypto_serving.pd.mpi_control import HOST, receive_rank_frame, send_rank_frame

logger = logging.getLogger(__name__)
ROUTER_TOKEN_ENV = "SIMPLER_MPI_DIRECT_ROUTER_TOKEN"
_ROUTABLE = {"on_demand", "prebuilt"}
_PAIR_STATES = _ROUTABLE | {"preparing", "failed", "retired"}


class MPISubscription:
    def __init__(self, deployment, withdraw, probe, drain):
        self.deployment = deployment
        self.withdraw, self.probe, self.drain = withdraw, probe, drain
        self.instances = {service.replica.name: (rank, service.replica.role, service.replica.base_url)
                          for rank, service in enumerate(deployment.services, 1)}
        self.ready = {}
        self.pair_status = {}
        self.descriptors = set()
        self.connected = False
        self.draining = False
        self.writer = self.task = None
        self.token = os.environ.get(ROUTER_TOKEN_ENV)
        if not self.token:
            raise ValueError(f"{ROUTER_TOKEN_ENV} is required for a Supervisor-owned deployment")

    async def start(self):
        self.task = asyncio.create_task(self._monitor())

    async def stop(self):
        if self.writer is not None and await self.drain(0) == 0 and not self.draining:
            with suppress(OSError, asyncio.TimeoutError):
                await asyncio.wait_for(send_rank_frame(self.writer, {
                    "type": "detached", "id": secrets.token_hex(16),
                }), 2)
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    def expects_descriptor(self, name):
        return self.connected and name in self.ready

    def matches_descriptor(self, node):
        return self.expects_descriptor(node.node_id) and self.ready[node.node_id] == (node.boot, node.registry)

    def confirm_descriptor(self, name):
        self.descriptors.add(name)

    def forget_descriptor(self, name):
        self.descriptors.discard(name)

    def allows_pair(self, prefill, decode):
        pair = self.pair_status.get((prefill, decode))
        return self.connected and not self.draining and pair is not None and pair["state"] in _ROUTABLE

    def status(self):
        return {"owner": "supervisor", "connected": self.connected, "draining": self.draining,
                "pairs": list(self.pair_status.values()),
                "instances": {
                    name: {"rank": rank, "role": role, "state": "ready" if name in self.ready else "pending",
                           "descriptor_verified": name in self.descriptors}
                    for name, (rank, role, _) in self.instances.items()
                }}

    def _snapshot(self, frame):
        fresh = {}
        pairs = {}
        if (not isinstance(frame.get("instances"), list) or not isinstance(frame.get("pairs"), list)
                or type(frame.get("draining")) is not bool):
            raise ValueError("invalid MPI pair snapshot")
        for item in frame["instances"]:
            name = item["id"]
            if (self.instances.get(name) != (item["rank"], item["role"], item["http_url"])
                    or name in fresh or not all(isinstance(item[k], str) and item[k]
                                                for k in ("boot", "registry_digest"))):
                raise ValueError("MPI snapshot differs from the configured instance identity")
            fresh[name] = (item["boot"], item["registry_digest"])
        for item in frame["pairs"]:
            key = p, d = item["prefill"], item["decode"]
            if (key in pairs or self.instances.get(p, (None, None))[1] != "prefill"
                    or self.instances.get(d, (None, None))[1] != "decode" or item["state"] not in _PAIR_STATES
                    or (item["state"] in _ROUTABLE and not all(name in fresh for name in key))):
                raise ValueError("invalid MPI pair state or identity")
            pairs[key] = item
        superseded = [name for name, identity in self.ready.items() if fresh.get(name) != identity]
        self.connected, self.draining = True, frame["draining"]
        self.ready, self.pair_status = fresh, pairs
        for name in superseded:
            self.withdraw(name)
        self.probe()

    async def _monitor(self):
        while True:
            try:
                await self._read_connection()
            except (OSError, ValueError, EOFError, KeyError, TypeError, asyncio.TimeoutError) as exc:
                if self.connected:
                    logger.warning("MPI routing subscription disconnected: %s", exc)
            finally:
                self.connected = False
                self.pair_status = {}
                for name in tuple(self.ready):
                    self.withdraw(name)
                self.ready.clear()
                if self.writer is not None:
                    self.writer.close()
                    self.writer = None
            await asyncio.sleep(0.2)

    async def _read_connection(self):
        reader, self.writer = await asyncio.open_connection(HOST, self.deployment.router_port)
        await send_rank_frame(self.writer, {"type": "hello", "token": self.token})
        reply = await asyncio.wait_for(receive_rank_frame(reader), self.deployment.auth_timeout_s)
        if reply != {"type": "hello", "rank": 0}:
            raise ValueError("unexpected rank-zero authentication reply")
        while True:
            frame = await receive_rank_frame(reader)
            if frame["type"] == "snapshot":
                self._snapshot(frame)
            elif frame["type"] == "drain":
                self.draining = True
                timeout = min(self.deployment.drain_timeout_s, float(frame["timeout_s"]))
                remaining = await self.drain(max(timeout, 0))
                await send_rank_frame(self.writer, {"type": "drain_result", "id": frame["id"],
                                                    "ok": remaining == 0, "remaining": remaining})
            else:
                raise ValueError("unexpected frame in MPI routing subscription")
