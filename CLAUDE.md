# kislayphp/socket — notes for AI assistants

Realtime transport extension (Engine.IO polling + WebSocket upgrade, rooms,
namespaces, ack callbacks, optional Redis pub/sub relay for horizontal
scaling). Single source file: `kislay_socket.cpp`, built on civetweb
(`third_party/civetweb`). PHP-facing classes live under `Kislay\Socket\*`
(with `Kislay\EventBus\*` / `KislayPHP\EventBus\*` kept as compatibility
aliases — this package is eventbus's active successor; new code should use
`Kislay\Socket\*`).

## Architecture (the part that isn't obvious from reading one function)

Every HTTP connection (long-poll GET/POST, or a WebSocket frame) is handled
by a civetweb worker thread. Those threads **never touch Zend directly** —
they only manipulate plain C++ state (`server->sessions`, `server->rooms`,
`server->clients`, all guarded by `server->lock`, a `std::mutex`) and push
parsed events onto `server->raw_event_queue`. A single dedicated dispatcher
thread drains that queue and is the *only* thread that ever calls into PHP
(`on()` handlers, `emitTo()`, etc.), serialized via
`kislay_socket_php_call_lock`. This split exists because the extension is
NTS-only — there's no per-thread Zend engine isolation, so only one thread
may ever be inside Zend at a time, but HTTP I/O (especially long-polling,
which blocks for up to `ping_interval_ms`) must not be serialized the same
way or every connection blocks every other one.

**Long-polling GET blocks** inside `cv.wait_for(lock, wait_ms, predicate)`
where `wait_ms` defaults to `ping_interval_ms` (25000ms) and the predicate
checks *that specific session's* `queue`. If the client gives up first (its
own HTTP client timeout) and closes the socket, the server has **no way to
notice** — it's parked in a condition-variable wait, oblivious to socket
state — so the worker thread stays occupied for up to the full wait window.

## The num_threads bug (fixed 2026-08-30, don't reintroduce it)

`KislaySocketServer::listen()` used to hardcode civetweb's `num_threads` to
`"1"`. Combined with the blocking-long-poll behavior above, this meant one
abandoned long-poll (a client that timed out and walked away) could starve
**every other concurrent connection on the same server** — including
message delivery to other, still-connected clients — for up to 25 seconds.
Reproduced deterministically: a 2-member room, one member leaves, the
*remaining* member self-sends a room broadcast while both still have an
open long-poll — the departed member's dead poll starved the remaining
member's own delivery. See `tests/room_broadcast_test.php`, which is now
the regression test for this. Fixed via `setThreads()` +
`KISLAYPHP_SOCKET_THREADS` env var (legacy fallback:
`KISLAYPHP_EVENTBUS_THREADS`), default raised to 4 — mirrors the pattern
`eventbus`'s own `kislay_socket.cpp` already used for the same civetweb
option. There was never a Zend-safety reason for capping at 1: the
dispatcher-thread split above already makes HTTP worker thread count
irrelevant to Zend safety.

**If you touch `listen()` or add new blocking behavior to the GET/POST
handlers, re-run `tests/room_broadcast_test.php` 5x** — this class of bug
reproduces 100% when present and 0% when absent; it is not flaky, so a
single clean run is a real signal, not luck.

## Testing

No `run_all.php` aggregator here (unlike eventbus). Tests are individual
PHP scripts under `tests/`, each spawning a real server subprocess via
`tests/_harness.php`'s `kislay_test_start_server()` and driving it with
real cURL (`kislay_test_http_post`/`kislay_test_http_get_async` +
`kislay_test_reap_async_get`). Run each directly:
```
php tests/connection_basics_test.php
php tests/room_broadcast_test.php
php tests/stress_reconnect_test.php
php tests/malloc_debug_repro_test.php   # informational only, see below
```
Standard `make test` (phpt) currently finds nothing to run — this module's
real coverage lives in the custom harness above, not phpt.

`malloc_debug_repro_test.php` is a deliberately-kept repro for a
previously-open WebSocket crash bug (see the socket_websocket_crash_fix
history) — it prints "0/N crashed... This is expected to be well above
0... it is not a pass/fail assertion" from when the bug was still open.
That comment is now stale (the underlying bug was fixed and reconfirmed at
0/140 crashes against live traffic); 0/N here is the *good* outcome, not a
sign the test is broken. Don't "fix" the comment without checking history
first — it's intentionally kept as a standing regression probe.

## Known open issues

None specific to this module as of 2026-08-30. See the top-level
`llms.txt` for ecosystem-wide open issues (e.g. `core`'s Darwin-only
`listenAsync()` SIGBUS, which is unrelated to this module).

## Gotchas for future changes

- `server->lock` (plain `std::mutex`) protects `sessions`/`rooms`/`clients`/
  `raw_event_queue`. `kislay_socket_php_call_lock` protects Zend access.
  Lock ordering (if you ever need both): `kislay_socket_php_call_lock`
  outermost — never take `server->lock` first and then try to enter Zend.
- `kislay_emit_room()` copies recipient sids into a local `vector` before
  iterating and sending, specifically to avoid iterator invalidation if a
  handler mutates `server->rooms` mid-broadcast. Preserve that pattern in
  any similar fan-out code.
- Every custom object struct (`php_kislay_socket_server_t`,
  `_client_t`, `_ack_t`, `_namespace_t`) must keep `zend_object std` as the
  **last member** — a struct-layout bug of exactly this shape caused a real
  heap-corruption crash in the `eventbus` sibling module; verified correct
  here but re-check after any struct edit.
