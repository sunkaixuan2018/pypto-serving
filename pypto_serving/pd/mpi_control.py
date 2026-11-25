# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the License.
# -----------------------------------------------------------------------------------------------------------
"""Local connection to a Simpler MPI service rank."""

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
import logging
import struct

import msgspec


HOST = "127.0.0.1"
MAX_FRAME = 8 * 1024 * 1024
PREFIX = "SIMPLER_MPI_DIRECT_"
logger = logging.getLogger("pypto_serving.pd")


def _port(value):
    if type(value) is not int or not 0 < value < 65536:
        raise ValueError("invalid MPI service port")
    return value


async def send_rank_frame(writer, value):
    payload = msgspec.json.encode(value)
    if not 0 < len(payload) <= MAX_FRAME:
        raise ValueError("MPI service frame is too large")
    writer.write(struct.pack("!I", len(payload)) + payload)
    await writer.drain()


async def receive_rank_frame(reader):
    length, = struct.unpack("!I", await reader.readexactly(4))
    if not 0 < length <= MAX_FRAME:
        raise ValueError("invalid MPI service frame length")
    frame = msgspec.json.decode(await reader.readexactly(length))
    if not isinstance(frame, dict):
        raise ValueError("MPI service frame must be an object")
    return frame


@dataclass(frozen=True)
class MPISettings:
    rank: int
    port: int
    token: str = field(repr=False)
    bind_port: int = 0
    peers: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_environment(cls, environ, *, node_id, role, decode_ids, provider):
        fields = ("HOST", "PORT", "TOKEN", "RANK", "INSTANCE_ID", "ROLE", "PEERS", "BIND_PORT")
        values = {name: environ[PREFIX + name] for name in fields if PREFIX + name in environ}
        if not values:
            return None
        if provider != "hixl":
            raise ValueError("MPI service control requires the HIXL PD provider")
        if (values.get("HOST") != HOST or values.get("INSTANCE_ID") != node_id
                or values.get("ROLE") != role or not values.get("TOKEN")):
            raise ValueError("MPI service identity differs from the configured PD instance")
        rank = int(values["RANK"])
        if rank < 1:
            raise ValueError("MPI service rank must be positive")
        port = _port(int(values["PORT"]))
        if role == "decode":
            return cls(rank, port, values["TOKEN"], bind_port=_port(int(values["BIND_PORT"])))
        peers = msgspec.json.decode(values["PEERS"])
        if not isinstance(peers, dict) or set(peers) != set(decode_ids):
            raise ValueError("MPI service peers must list every configured Decode instance")
        return cls(rank, port, values["TOKEN"], peers={name: _port(value) for name, value in peers.items()})


class MPIServiceControl:
    def __init__(self, settings, timeout, request_shutdown):
        self.settings = settings
        self.timeout = timeout
        self.request_shutdown = request_shutdown
        self.writer = None
        self.task = None
        self.ready = False

    @property
    def bind_address(self):
        return HOST, self.settings.bind_port

    def connect_address(self, peer_id):
        return HOST, self.settings.peers[peer_id]

    async def start(self, node):
        try:
            await asyncio.wait_for(self._start(node), self.timeout)
        except BaseException:
            await self.close()
            raise

    async def _start(self, node):
        reader, self.writer = await asyncio.open_connection(HOST, self.settings.port)
        await send_rank_frame(self.writer, {"type": "hello", "token": self.settings.token})
        reply = await receive_rank_frame(reader)
        if reply != {"type": "hello", "rank": self.settings.rank}:
            raise ConnectionError("MPI service rank handshake failed")
        await send_rank_frame(self.writer, {"type": "status", "state": "ready", "payload": {
            "boot": node.boot, "registry_digest": node.registry,
        }})
        self.ready = True
        self.task = asyncio.create_task(self._listen(reader))

    async def _listen(self, reader):
        try:
            while True:
                frame = await receive_rank_frame(reader)
                if frame.get("type") == "shutdown":
                    self.ready = False
                    self.request_shutdown()
                elif frame.get("type") == "broadcast":
                    logger.info("MPI service broadcast id=%s scope=%s", frame.get("id"), frame.get("scope"))
                else:
                    raise ValueError("unexpected MPI service command")
        except (OSError, ValueError, asyncio.IncompleteReadError):
            logger.warning("MPI service control disconnected", exc_info=True)
        finally:
            self.ready = False

    async def close(self):
        self.ready = False
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        if self.writer is not None:
            self.writer.close()
            with suppress(OSError, asyncio.TimeoutError):
                await asyncio.wait_for(self.writer.wait_closed(), self.timeout)
