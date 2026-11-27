# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the License.
# -----------------------------------------------------------------------------------------------------------
"""Verify the MPI rank IPC and unchanged Serving peer handshake."""

import asyncio
from types import SimpleNamespace

import pytest

from pypto_serving.pd.config import Config, Endpoint
from pypto_serving.pd.mpi_control import (
    HOST,
    MPIServiceControl,
    MPISettings,
    PREFIX,
    receive_rank_frame,
    send_rank_frame,
)
from pypto_serving.pd.protocol import Component, Contract, Node, Topology, digest, receive, send
from pypto_serving.pd.service import Service


def test_settings_are_opt_in_and_validate_all_destination_ports():
    def read(values):
        return MPISettings.from_environment(
            {PREFIX + key: value for key, value in values.items()},
            node_id="p", role="prefill", decode_ids=("d",), provider="hixl",
        )

    assert read({}) is None
    settings = dict(HOST=HOST, PORT="3456", TOKEN="test-token", RANK="1",
                    INSTANCE_ID="p", ROLE="prefill", PEERS='{"d":3457}')
    assert read(settings).peers == {"d": 3457}
    assert "test-token" not in repr(read(settings))
    with pytest.raises(ValueError, match="every configured Decode instance"):
        read(dict(settings, PEERS="{}"))
    with pytest.raises(ValueError, match="MPI service identity"):
        read(dict(settings, HOST="192.0.2.1"))


def test_rank_readiness_uses_boot_and_registry_and_shutdown_demotes_node():
    async def scenario():
        status = asyncio.get_running_loop().create_future()
        shutdown = asyncio.Event()
        release = asyncio.Event()

        async def rank(reader, writer):
            try:
                assert await receive_rank_frame(reader) == {"type": "hello", "token": "test-token"}
                await send_rank_frame(writer, {"type": "hello", "rank": 1})
                status.set_result(await receive_rank_frame(reader))
                await release.wait()
                await send_rank_frame(writer, {"type": "shutdown"})
                assert await reader.read() == b""
            finally:
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(rank, HOST, 0) as listener:
            mpi = MPIServiceControl(MPISettings(1, listener.sockets[0].getsockname()[1], "test-token"),
                                    2, shutdown.set)
            try:
                await mpi.start(SimpleNamespace(boot="boot-p", registry="registry-p"))
                assert mpi.ready
                assert await asyncio.wait_for(status, 2) == {
                    "type": "status", "state": "ready",
                    "payload": {"boot": "boot-p", "registry_digest": "registry-p"},
                }
                release.set()
                await asyncio.wait_for(shutdown.wait(), 2)
                assert not mpi.ready
            finally:
                await mpi.close()

    asyncio.run(scenario())


def test_peer_handshake_uses_local_relay_address_and_registry():
    async def scenario():
        topology = Topology.for_tp(1, 1)
        registry = {"topology": topology}
        contract = Contract("model", "model", "layout", (Component("kv", "kv", "kv"),))
        p = Node("p", "prefill", "boot-p", contract, digest(registry), topology, "hixl")
        d = Node("d", "decode", "boot-d", contract, digest(registry), topology, "hixl")

        async def relay(reader, writer):
            try:
                hello = await receive(reader)
                assert hello["node"]["node_id"] == "p"
                await send(writer, {"node": d, "registry": registry})
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(relay, HOST, 0) as listener:
            config = Config((Endpoint(HOST, 8111, "p"),), (Endpoint("not-a-real-host", 8112, "d"),),
                            provider="hixl")
            mpi = MPIServiceControl(MPISettings(1, 1, "test-token",
                                               peers={"d": listener.sockets[0].getsockname()[1]}),
                                    2, lambda: None)
            service = Service(config, config.prefill[0], "prefill", None,
                              SimpleNamespace(kv_cache_manager=None, is_ready=lambda: True), None,
                              mpi_control=mpi)
            service.node, service.registry = p, registry
            assert not service.is_ready()
            mpi.ready = True
            assert service.is_ready()
            _, writer, remote_registry = await asyncio.wait_for(service._connect_peer(d), 2)
            writer.close()
            await writer.wait_closed()
            assert digest(remote_registry) == d.registry

    asyncio.run(scenario())
