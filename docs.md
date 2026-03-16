# Kislay Socket Technical Reference

## What It Is

Kislay Socket is the transport package for realtime, long-running socket communication in KislayPHP. It exposes a blocking server runtime with Engine.IO polling support, optional WebSocket upgrade, rooms, namespaces, per-client replies, and broadcast events.

Primary namespace:
- `Kislay\\Socket`

Compatibility namespaces during `0.0.x`:
- `Kislay\\EventBus`
- `KislayPHP\\EventBus`
- `KislayPHP\\Socket`

## Runtime Model

```text
Client -> HTTP polling or WebSocket -> Kislay\Socket\Server -> PHP event handlers
```

The server owns:
- session lifecycle
- handshake state
- polling queue
- room membership
- namespace routing
- ping/pong liveness

The PHP layer handles:
- connection callbacks
- event handlers
- auth callbacks
- room joins and leaves

## Core API

### `Kislay\\Socket\\Server`

```php
$server = new Kislay\Socket\Server();
```

Methods:
- `on(string $event, callable $handler): bool`
- `emit(string $event, mixed $data): bool`
- `publish(string $event, mixed $data): bool`
- `send(string $event, mixed $data): bool`
- `emitTo(string $room, string $event, mixed $data): bool`
- `listen(string $host, int $port, string $path): bool`
- `clientCount(): int`
- `roomCount(string $room): int`
- `onAuth(callable $handler): bool`
- `onWithAck(string $event, callable $handler): bool`
- `getClients(): array`
- `setMaxPayload(int $bytes): bool`
- `namespace(string $ns): Kislay\\Socket\\Namespace`

### `Kislay\\Socket\\Socket`

Methods:
- `id(): string`
- `join(string $room): bool`
- `leave(string $room): bool`
- `emit(string $event, mixed $data): bool`
- `publish(string $event, mixed $data): bool`
- `send(string $event, mixed $data): bool`
- `reply(string $event, mixed $data): bool`
- `emitTo(string $room, string $event, mixed $data): bool`

Semantics:
- `Server::emit()` broadcasts to all clients.
- `Socket::emit()` sends to the current client only.
- `Socket::emitTo()` broadcasts to a room.

### `Kislay\\Socket\\Namespace`

Methods:
- `on(string $event, callable $handler): bool`
- `onWithAck(string $event, callable $handler): bool`
- `emit(string $event, mixed $data): bool`
- `emitTo(string $sid, string $event, mixed $data): bool`
- `emitToRoom(string $room, string $event, mixed $data): bool`
- `join(string $sid, string $room): bool`

## Minimal Example

```php
<?php

$server = new Kislay\Socket\Server();

$server->on('connection', function (Kislay\Socket\Socket $socket) {
    $socket->join('lobby');
    $socket->reply('welcome', ['id' => $socket->id()]);
});

$server->on('chat', function (Kislay\Socket\Socket $socket, array $payload) {
    $socket->emitTo('lobby', 'chat', [
        'from' => $socket->id(),
        'message' => $payload['message'] ?? '',
    ]);
});

$server->listen('0.0.0.0', 3000, '/socket.io/');
```

## Polling and WebSocket

The server accepts Engine.IO-style polling requests and WebSocket upgrades on the same path.

Recommended path:
- `/socket.io/`

Behavior:
- polling works as the initial transport
- WebSocket upgrade is allowed when enabled
- if upgrade is disabled, polling still works

## Authentication

```php
$server->onAuth(function (array $headers, string $query): bool {
    parse_str($query, $params);
    return ($params['token'] ?? '') === 'secret';
});
```

Auth-related environment variables:
- `KISLAYPHP_SOCKET_AUTH_ENABLED`
- `KISLAYPHP_SOCKET_AUTH_TOKEN`
- `KISLAYPHP_SOCKET_AUTH_QUERY_KEYS`
- `KISLAYPHP_SOCKET_AUTH_HEADER_KEYS`

## Environment Variables

- `KISLAYPHP_SOCKET_ALLOW_UPGRADE`
- `KISLAYPHP_SOCKET_CORS`
- `KISLAYPHP_SOCKET_PING_INTERVAL_MS`
- `KISLAYPHP_SOCKET_PING_TIMEOUT_MS`
- `KISLAYPHP_SOCKET_MAX_PAYLOAD`
- `KISLAYPHP_SOCKET_TRANSPORTS`

Legacy compatibility environment variables are still accepted during `0.0.x`:
- `KISLAYPHP_EVENTBUS_*`
- `KISLAYPHP_AUTH_*`

## Current Limits

- blocking server loop
- single in-process server instance
- no durable replay/history
- no distributed transport bridge here by default

## Use It For

- realtime UI events
- room-based messaging
- browser-to-service socket flows
- service socket endpoints where low-latency push matters

## Do Not Use It As

- a durable queue
- a Kafka-style event log
- a background worker system
- a multi-node durable event backbone

For those cases:
- use `kislayphp/queue` for jobs
- keep higher-level distributed event semantics separate from this transport layer
