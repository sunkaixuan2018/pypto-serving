# Copyright (c) PyPTO Contributors.
# This program is free software, you can redistribute it and/or modify it under the terms and conditions of
# CANN Open Software License Agreement Version 2.0 (the "License").
# Please refer to the License for details. You may not use this file except in compliance with the License.
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE.
# See LICENSE in the root of the software repository for the full text of the License.
# -----------------------------------------------------------------------------------------------------------

"""Placement and launch options for a Router-owned MPI Direct deployment."""

from __future__ import annotations

import ipaddress
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import ReplicaSpec


PD_CONFIG_ENV = "PYPTO_MPI_PD_CONFIG"
PD_CONFIG_ARG = "mpi://deployment"
_MANAGED_FLAGS = {"--host", "--port", "--devices", "--device", "--pd-config", "--pd-role", "--pd-node-id"}
_DEPLOYMENT_FIELDS = {
    "controller_host", "router_port", "services", "mpirun", "launcher_family", "launcher_args",
    "supervisor_python", "rank_python", "auth_timeout_s", "startup_timeout_s", "session_timeout_s",
    "shutdown_timeout_s", "drain_timeout_s", "log_dir",
    "owner", "prebuild", "prepare_timeout_s",
}


def _text(value, name):
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _host(value, name):
    host = _text(value, name).lower().removesuffix(".")
    if host == "localhost":
        return "127.0.0.1"
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host):
            raise ValueError(f"{name} must be an IPv4 address or hostname") from None
        return host
    if address.version != 4 or address.is_unspecified:
        raise ValueError(f"{name} must be a concrete IPv4 address or hostname")
    return "127.0.0.1" if address.is_loopback else str(address)


def _port(value, name):
    if type(value) is not int or not 1 <= value <= 65535:
        raise ValueError(f"{name} must be a TCP port in 1..65535")
    return value


def _seconds(data, name, default):
    value = data.get(name, default)
    if type(value) not in (int, float) or not 0 < value < math.inf:
        raise ValueError(f"mpi_pd.{name} must be finite and positive")
    return float(value)


def _strings(value, name):
    if not isinstance(value, list) or not all(isinstance(arg, str) and "\0" not in arg for arg in value):
        raise ValueError(f"{name} must be a string array")
    return tuple(value)


@dataclass(frozen=True)
class MPIService:
    replica: ReplicaSpec
    host: str
    devices: tuple[int, ...]
    argv: tuple[str, ...]
    env: dict[str, str]
    cwd: str | None
    bind_port: int | None

    @classmethod
    def parse(cls, replica, data):
        label = f"mpi_pd.services[{replica.name!r}]"
        if not isinstance(data, dict) or set(data) - {"devices", "argv", "env", "cwd"}:
            raise ValueError(f"{label} requires a service object with devices and argv")
        devices = data.get("devices")
        if (not isinstance(devices, list) or not devices
                or any(type(d) is not int or d < 0 for d in devices) or len(set(devices)) != len(devices)):
            raise ValueError(f"{label}.devices must be a nonempty list of distinct nonnegative device IDs")
        argv = _strings(data.get("argv"), f"{label}.argv")
        if not argv or not argv[0].strip():
            raise ValueError(f"{label}.argv must include an executable")
        flags = {arg.split("=", 1)[0] for arg in argv[1:] if arg.startswith("--")}
        if "--" in flags or flags & _MANAGED_FLAGS:
            raise ValueError(f"{label}.argv must leave host, port, device and PD flags to the Router")
        env = data.get("env", {})
        if not isinstance(env, dict) or any(
            not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", key)
            or key == PD_CONFIG_ENV or key.startswith("SIMPLER_MPI_DIRECT_")
            or not isinstance(value, str) or "\0" in value for key, value in env.items()
        ):
            raise ValueError(f"{label}.env must map environment names to strings and leave MPI variables reserved")
        cwd = data.get("cwd")
        if cwd is not None:
            _text(cwd, f"{label}.cwd")
        host = _host(replica.host, f"{replica.name}.host")
        bind_port = None
        if replica.role == "decode":
            bind_port = _port(replica.control_port or replica.port + 10000, f"{replica.name}.control_port")
        return cls(replica, host, tuple(devices), argv, dict(env), cwd, bind_port)

    def record(self, snapshot):
        spec = self.replica
        value = {
            "id": spec.name,
            "role": spec.role,
            "host": self.host,
            "argv": [*self.argv, "--host", spec.host, "--port", str(spec.port),
                     "--devices", ",".join(map(str, self.devices)), "--pd-role", spec.role,
                     "--pd-node-id", spec.name, "--pd-config", PD_CONFIG_ARG],
            "env": {**self.env, PD_CONFIG_ENV: snapshot},
        }
        if self.cwd is not None:
            value["cwd"] = self.cwd
        if self.bind_port is not None:
            value["bind_port"] = self.bind_port
        return value


@dataclass(frozen=True)
class MPIDeployment:
    controller_host: str
    router_port: int
    services: tuple[MPIService, ...]
    mpirun: str
    launcher_family: str
    launcher_args: tuple[str, ...]
    supervisor_python: str
    rank_python: str | None
    auth_timeout_s: float
    startup_timeout_s: float
    session_timeout_s: float
    shutdown_timeout_s: float
    drain_timeout_s: float
    log_dir: Path
    owner: str
    prebuild: tuple[dict, ...]
    prepare_timeout_s: float

    @classmethod
    def parse(cls, data, replicas):
        if not isinstance(data, dict) or set(data) - _DEPLOYMENT_FIELDS:
            raise ValueError("mpi_pd must be an object containing only MPI deployment fields")
        if {r.role for r in replicas} != {"prefill", "decode"}:
            raise ValueError("mpi_pd requires prefill and decode replicas")
        by_id = data.get("services")
        if not isinstance(by_id, dict) or set(by_id) != {r.name for r in replicas}:
            raise ValueError("mpi_pd.services must key one launch entry per replica ID")
        controller = _host(data.get("controller_host", "127.0.0.1"), "mpi_pd.controller_host")
        port = _port(data.get("router_port"), "mpi_pd.router_port")
        groups = {controller: []}
        devices_by_host = {}
        ports = {(controller, port): "mpi_pd.router_port"}
        for replica in replicas:
            service = MPIService.parse(replica, by_id[replica.name])
            host = service.host
            groups.setdefault(host, []).append(service)
            used = devices_by_host.setdefault(host, set())
            if used.intersection(service.devices):
                raise ValueError(f"{replica.name} overlaps another MPI device group on {host}")
            used.update(service.devices)
            for candidate in (replica.port, service.bind_port):
                if candidate is None:
                    continue
                key = (host, _port(candidate, f"{replica.name}.port"))
                if key in ports:
                    raise ValueError(f"{replica.name} port {candidate} on {host} conflicts with {ports[key]}")
                ports[key] = replica.name
        if len(groups) > 1 and "127.0.0.1" in groups:
            raise ValueError("MPI hosts must use reachable addresses in a multi-host deployment")
        family = data.get("launcher_family", "auto")
        if family not in ("auto", "openmpi", "mpich"):
            raise ValueError("mpi_pd.launcher_family must be auto, openmpi or mpich")
        startup = _seconds(data, "startup_timeout_s", 1200)
        auth = _seconds(data, "auth_timeout_s", min(60, startup))
        if auth > startup:
            raise ValueError("mpi_pd.auth_timeout_s cannot exceed startup_timeout_s")
        session = _seconds(data, "session_timeout_s", 60)
        rank_python = data.get("rank_python")
        if rank_python is not None:
            _text(rank_python, "mpi_pd.rank_python")
        owner = data.get("owner", "router")
        if owner not in ("router", "supervisor"):
            raise ValueError("mpi_pd.owner must be router or supervisor")
        if owner == "router" and ("prebuild" in data or "prepare_timeout_s" in data):
            raise ValueError("MPI prebuild options require Supervisor ownership")
        services = tuple(s for group in groups.values() for s in group)
        prebuild = _prebuild(data.get("prebuild", []), {s.replica.name: s for s in services})
        return cls(
            controller, port, services,
            _text(data.get("mpirun", "mpirun"), "mpi_pd.mpirun"), family,
            _strings(data.get("launcher_args", []), "mpi_pd.launcher_args"),
            _text(data.get("supervisor_python", sys.executable), "mpi_pd.supervisor_python"), rank_python,
            auth, startup, session, _seconds(data, "shutdown_timeout_s", 2 * session + 48),
            _seconds(data, "drain_timeout_s", 300),
            Path(_text(data.get("log_dir", "serving_log/mpi_pd"), "mpi_pd.log_dir")),
            owner, prebuild, _seconds(data, "prepare_timeout_s", 120),
        )

    def validate_http_bind(self, port):
        if port == self.router_port or any(
            s.host == self.controller_host and port in (s.replica.port, s.bind_port) for s in self.services
        ):
            raise ValueError("Router HTTP port conflicts with an MPI endpoint on the controller host")

    def manifest(self, config, pd_config):
        if pd_config.provider != "hixl":
            raise ValueError("mpi_pd requires pd.provider hixl")
        snapshot = json.dumps({
            "replicas": [asdict(replica) for replica in config.replicas],
            "pd": {**(config.pd_options or {}), "run_id": pd_config.run_id},
            "observability": config.observability or {},
        }, separators=(",", ":"), allow_nan=False)
        # execve on Linux limits each individual variable to 128 KiB on 4 KiB pages.
        if len(snapshot) > 120 * 1024:
            raise ValueError("MPI PD endpoint configuration is too large for the process environment")
        manifest = {
            "controller_host": self.controller_host,
            "router_port": self.router_port,
            "startup_timeout_s": self.startup_timeout_s,
            "session_timeout_s": self.session_timeout_s,
            "launcher_args": list(self.launcher_args),
            "services": [s.record(snapshot) for s in self.services],
        }
        if self.owner == "supervisor":
            for service, record in zip(self.services, manifest["services"]):
                record["http_url"] = service.replica.base_url
            manifest["coordination"] = {
                "prebuild": list(self.prebuild), "prepare_timeout_s": self.prepare_timeout_s,
                "drain_timeout_s": self.drain_timeout_s,
            }
        return manifest


def _prebuild(entries, services):
    if not isinstance(entries, list):
        raise ValueError("mpi_pd.prebuild must be an array")
    configured = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {"prefill", "decode", "device_pairs"}:
            raise ValueError("invalid MPI prebuild entry")
        nodes = []
        for role in ("prefill", "decode"):
            name = entry.get(role)
            service = services.get(name) if isinstance(name, str) else None
            if service is None or service.replica.role != role:
                raise ValueError(f"MPI prebuild requires a configured {role} ID")
            nodes.append(service)
        p, d = nodes
        key = p.replica.name, d.replica.name
        pairs = entry.get("device_pairs", [])
        if (key in configured or not isinstance(pairs, list) or any(
            not isinstance(pair, list) or len(pair) != 2 or any(
                type(rank) is not int or not 0 <= rank < len(service.devices)
                for rank, service in zip(pair, nodes)
            ) for pair in pairs
        )):
            raise ValueError("duplicate MPI prebuild or invalid device rank pair")
        if len({tuple(pair) for pair in pairs}) != len(pairs):
            raise ValueError("duplicate MPI prebuild device rank pair")
        configured[key] = {**entry, "device_pairs": [list(pair) for pair in pairs]}
    return tuple(configured.values())
