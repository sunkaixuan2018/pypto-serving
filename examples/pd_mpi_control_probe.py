# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the license.
# -----------------------------------------------------------------------------------------------------------
"""CPU-only 1P1D client for the Simpler MPI Direct service launcher."""

import asyncio
import json
import logging
import os
import uuid

from pypto_serving.pd.mpi_control import MPIServiceControl, MPISettings, PREFIX
from pypto_serving.pd.protocol import Component, Contract, Node, Topology, decode, digest, receive, send
from pypto_serving.pd.session import Session


def local_node(node_id, role):
    boot = uuid.uuid4().hex
    topology = Topology.for_tp(1, 1)
    registry = {"topology": topology, "boot": boot, "data": "r" * (96 * 1024)}
    contract = Contract("probe", "probe", "probe", (Component("kv", "kv", "kv"),))
    return Node(node_id, role, boot, contract, digest(registry), topology, "hixl"), registry


def validate_hello(local, hello, peer_id=None):
    peer = decode(hello["node"], Node)
    if not local.compatible(peer) or (peer_id is not None and peer.node_id != peer_id):
        raise ValueError("peer is incompatible with this probe")
    if digest(hello["registry"]) != peer.registry or hello["registry"]["boot"] != peer.boot:
        raise ValueError("peer registry differs from the declared identity")
    return peer


async def serve_decode(reader, writer, node, registry):
    try:
        peer = validate_hello(node, await receive(reader))
        await send(writer, {"node": node, "registry": registry})
        plan = None
        while True:
            message = await receive(reader)
            op, data = message["op"], message["payload"]
            if op == "begin":
                result = {"offset": 4096, "length": 512}
            elif op == "plan":
                if data["boot"] != node.boot or data["placement"] != {"offset": 4096, "length": 512}:
                    raise ValueError("invalid destination binding")
                plan = digest(data)
                result = {"authorized_plan": plan}
            elif op == "complete":
                if plan is None or data["plan"] != plan:
                    raise ValueError("request completion is not authorized")
                result = {"committed": plan}
                plan = None
            elif op == "retire_link":
                if plan is not None:
                    raise ValueError("request is still active")
                await send(writer, {"id": message["id"], "result": {"retired": peer.boot}})
                return
            else:
                raise ValueError(f"unsupported operation {op!r}")
            await send(writer, {"id": message["id"], "result": result})
    finally:
        writer.close()
        await writer.wait_closed()


async def run_prefill(control, node, registry, peer_id):
    while True:
        reader, writer = await asyncio.open_connection(*control.connect_address(peer_id))
        try:
            await send(writer, {"node": node, "registry": registry})
            peer = validate_hello(node, await receive(reader), peer_id)
            break
        except (ConnectionError, asyncio.IncompleteReadError):
            writer.close()
            await asyncio.sleep(0.1)
        except BaseException:
            writer.close()
            raise
    session = Session(reader, writer)
    try:
        request = uuid.uuid4().hex
        placement = await session.call("begin", {"request": request})
        plan = {"request": request, "boot": peer.boot, "placement": placement}
        authorized = (await session.call("plan", plan))["authorized_plan"]
        assert authorized == digest(plan)
        assert await session.call("complete", {"plan": authorized}) == {"committed": authorized}
        assert await session.call("retire_link", {}) == {"retired": node.boot}
        print(f"PD_MPI_PROBE_PASS request={request} plan={authorized} destination={peer.node_id}", flush=True)
    finally:
        await session.close()


async def main():
    node_id, role = os.environ[PREFIX + "INSTANCE_ID"], os.environ[PREFIX + "ROLE"]
    peers = tuple(json.loads(os.environ.get(PREFIX + "PEERS", "{}")))
    settings = MPISettings.from_environment(os.environ, node_id=node_id, role=role,
                                            decode_ids=peers, provider="hixl")
    stop = asyncio.Event()
    control = MPIServiceControl(settings, 30, stop.set)
    node, registry = local_node(node_id, role)
    if role == "decode":
        listener = await asyncio.start_server(
            lambda reader, writer: serve_decode(reader, writer, node, registry), *control.bind_address,
        )
    else:
        listener = None
    try:
        await control.start(node)
        if role == "prefill":
            await asyncio.wait_for(run_prefill(control, node, registry, peers[0]), 20)
        await asyncio.wait_for(stop.wait(), 90)
    finally:
        if listener is not None:
            listener.close()
            await listener.wait_closed()
        await control.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
