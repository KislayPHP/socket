# Kislay Socket

Kislay Socket is the realtime socket transport package for KislayPHP. It provides long-running socket communication with Engine.IO polling, WebSocket upgrade support, rooms, namespaces, and event callbacks.

During `0.0.x`, the package keeps compatibility aliases for existing EventBus namespaces:
- `Kislay\\Socket\\*` is the primary API
- `Kislay\\EventBus\\*` and `KislayPHP\\EventBus\\*` remain available as compatibility aliases

## Versioning

This package stays on the `0.0.x` line until the transport surface, docs, and ecosystem integration are production-ready.

## Installation

### PIE

```bash
pie install kislayphp/socket:0.0.1
```

Add to `php.ini`:

```ini
extension=kislayphp_socket.so
```

### Build from source

```bash
git clone https://github.com/KislayPHP/socket.git
cd socket
phpize
./configure --enable-kislayphp_socket
make
sudo make install
```

## Minimal Server

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

## Public API

### `Kislay\\Socket\\Server`

- `on(string $event, callable $handler): bool`
- `emit(string $event, mixed $data): bool`
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

- `id(): string`
- `join(string $room): bool`
- `leave(string $room): bool`
- `emit(string $event, mixed $data): bool`
- `reply(string $event, mixed $data): bool`
- `emitTo(string $room, string $event, mixed $data): bool`

Behavior notes:
- `Server::emit()` broadcasts to all connected clients.
- `Socket::emit()` sends only to the current client.
- `Socket::reply()` is an alias for per-client emit.
- `listen()` blocks until the server stops.

## Configuration

Primary environment variables:

- `KISLAYPHP_SOCKET_PING_INTERVAL_MS`
- `KISLAYPHP_SOCKET_PING_TIMEOUT_MS`
- `KISLAYPHP_SOCKET_MAX_PAYLOAD`
- `KISLAYPHP_SOCKET_CORS`
- `KISLAYPHP_SOCKET_ALLOW_UPGRADE`
- `KISLAYPHP_SOCKET_TRANSPORTS`
- `KISLAYPHP_SOCKET_AUTH_ENABLED`
- `KISLAYPHP_SOCKET_AUTH_TOKEN`
- `KISLAYPHP_SOCKET_AUTH_QUERY_KEYS`
- `KISLAYPHP_SOCKET_AUTH_HEADER_KEYS`

Legacy `KISLAYPHP_EVENTBUS_*` and `KISLAYPHP_AUTH_*` environment variables are still accepted during `0.0.x` for migration compatibility.

## Current Limits

- in-process single server runtime
- no durable message history
- no multi-node transport bridge in this package yet
- at this stage, treat it as the socket transport layer, not as a durable event platform
- **do not load `socket` together with `core` and/or `gateway` in the same PHP process.** All three vendor their own copy of civetweb and export non-static symbols like `mg_start`; on platforms linking extensions with `-flat_namespace` (notably macOS), combining any two of them risks one's compiled civetweb code silently shadowing another's, with no error - just undefined behavior up to and including crashes. Run socket as its own process (e.g. behind gateway, or alongside core's process rather than inside it).

## Positioning

Use `kislayphp/socket` for:
- realtime browser or service socket connections
- rooms and namespaces
- request/response style live events
- transport-level event delivery

Use `kislayphp/queue` for:
- background jobs
- retries and DLQ
- worker/server queue processing

Use future higher-level EventBus semantics only for distributed event distribution, not for the socket transport package itself.
