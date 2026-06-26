# Platform Documentation

The `platform/` subtree contains the first-party C++ platform-management layer for PyPTO Serving. It is separate from the Python model-serving path and is intended to manage distributed-system bootstrap, deployment metadata, channel lifecycle, module services, and instance lifecycle.

This layer follows the design split described in GitHub issues #32 and #13:

- The platform starts and manages the distributed system.
- Model support keeps ownership of LLM-specific behavior such as batching, KV cache policy, token scheduling, sampling, and PyPTO/Simpler execution.
- Platform code should not sit in the per-token execution hot path.
- Host-side tasks should orchestrate, supervise, and exchange control metadata.
- Device-side runtime instances should perform model execution and tensor movement when the model data path is integrated.

## Source Layout

```text
platform/
  include/modules/                  module interfaces and platform modules
  include/modules/configuration/    deployment graph configuration types
  include/system/                   engine lifecycle and system utilities
  include/system/channels/          HiCR-backed channel primitives
  examples/                         executable examples for modules and engine lifecycle
  extern/                           vendored dependencies; not documented here
  build/                            generated build output; not documented here
```

## Module Areas

- [Modules](modules/README.md): common module lifecycle and first-party modules.
- [Configuration](modules/configuration.md): deployment graph objects and validation rules.
- [Service Module](modules/service.md): Periodic service aggregation.
- [Channel Controller](modules/channel-controller.md): desired-state channel reconciliation.
- [Broadcast Deployment](modules/broadcast-deployment.md): deployer-to-worker deployment distribution.
- [System](system/README.md): engine lifecycle and cross-instance start/stop control.
- [Channels](system/channels.md): payload, coordination, metadata, input, output, and message primitives.
- [Examples](examples/README.md): example programs and what they demonstrate.

## Runtime Shape

The current platform runtime is built around `serving::system::Engine`. The engine owns a set of `serving::modules::Module` instances, initializes them, starts them across instances through RPC, waits for termination, and finalizes them.

The implemented platform pieces already cover these issue #32 building blocks:

- Deployment graph representation through `Deployment`, `Partition`, `Task`, `Replica`, `Edge`, and `RequestManager`.
- Deployment broadcast from a deployer instance to other instances.
- Desired-state channel creation through `channelController::Module`.
- Host-side service scheduling through TaskR.
- Control/message metadata through `Message` and `MessageTypeRegistry`.

Dynamic scaling, topology-aware replacement, fault recovery, and Python bindings are not implemented in this subtree yet.
