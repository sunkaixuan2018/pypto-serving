# Channels

Source paths:

- `include/system/channels/base.hpp`
- `include/system/channels/input.hpp`
- `include/system/channels/output.hpp`
- `include/system/channels/message.hpp`
- `include/system/channels/messageTypeRegistry.hpp`

The channel layer wraps SPSC channels for platform messages. It separates data payload movement from coordination and metadata movement.

## Channel Base

`serving::system::channels::Base` owns common channel state:

- Channel configuration built from `configuration::Edge`.
- Global memory-slot keys.
- Local coordination buffers.
- Exchanged global memory slots.
- Ready state.
- A mutex for locking around channel operations.

`Base::initialize(tag)` resolves exchanged global slots through the configured communication managers, creates concrete producer/consumer channels, and marks the channel ready.

## Channel Configuration

`channelConfig_t` contains:

- Channel name.
- Buffer capacity and size.
- Payload communication manager, memory manager, and memory space.
- Coordination communication manager, memory manager, and memory space.

This split allows payload buffers and coordination buffers to use different HiCR managers or memory spaces.

## Input

`serving::system::channels::Input` is the consumer side.

It allocates:

- Consumer data sizes buffer.
- Consumer data payload buffer.
- Consumer metadata payload buffer.

It exposes:

- `hasMessage()` to check metadata and data channel depth.
- `getMessage()` to peek at the current payload and metadata.
- `popMessage()` to consume the current message.
- `getSourceInstance()` to identify the producer instance.

## Output

`serving::system::channels::Output` is the producer side.

It allocates:

- Producer size-info buffer for variable-size payloads.

It exposes:

- `isFull(messageSize)` to check capacity.
- `pushMessage(message)` to send a payload and metadata pair.
- `pushMessageLocking(message)` to wait until there is room and send with locking.
- `getTargetInstance()` to identify the consumer instance.

## Message

`serving::system::channels::Message` is a non-owning wrapper around a payload pointer, payload size, and packed metadata.

Metadata fields:

- `type`: 10-bit message type.
- `groupId`: 20-bit group id.
- `sequenceId`: 34-bit sequence id.

`metadata_t::getId()` packs those fields into a single 64-bit id.

## Message Type Registry

`MessageTypeRegistry` maps string message-type names to `Message::messageType_t` values using FNV-1a hashing truncated to the message type bit range.

The registry rejects:

- Empty names.
- Duplicate names.
- Hash collisions.
- Lookup of unregistered names or ids.

## Relation To Platform Design

The channel layer is the implementation foundation for platform control and data channels. Issue #32 distinguishes host control traffic from device tensor payload traffic. The current code already separates payload buffers from coordination/metadata buffers, but high-level host/device placement policy is not yet exposed as a first-class channel kind.

## Current Limitations

- `Message` does not own payload memory; callers must ensure payload lifetime is valid during push or read.
- Channels are SPSC-oriented.
