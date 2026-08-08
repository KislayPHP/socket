# Chat Demo (event-driven, browser-based)

A small multi-room chat app proving out `Kislay\Socket\Server::on()` +
`$client->join()`/`emitTo()` end-to-end against a real browser frontend —
`socket/example.php` and `socket/service_communication.php` already show
this idiom for a single fixed room; this demo extends it to a client-chosen
room name plus a real UI, rather than a curl-only proof.

## What it demonstrates

- `on('connection', ...)` / `on('disconnect', ...)` — 1-arg handlers.
- `on('join_room', ...)` / `on('chat_message', ...)` — 2-arg handlers
  (`$client`, `$data`), the second calling `$client->join($room)`.
- `$client->emitTo($room, ...)` for room-scoped broadcast, `$client->emit(...)`
  for a reply to just the caller.
- A hand-rolled Engine.IO/Socket.IO v2 text-framing client in vanilla JS
  (`public/app.js`) over a raw `WebSocket` — no official JS client ships
  with this repo, and the server applies the same packet parser to
  WebSocket frames as it does to polling requests, so the framing is
  required even over a native `WebSocket` connection.

## Known limitations

**This demo supports exactly one connected client at a time.**
`Kislay\Socket\Server::listen()` currently hardcodes civetweb's
`num_threads` to `1`, meaning the whole server can only service ONE
connection process-wide — a second client's connection attempt hangs until
the first disconnects (confirmed directly, not theoretical). This isn't
something this demo introduced; every previous use of this extension only
ever drove one connection at a time, so the ceiling never surfaced until a
real multi-tab browser session was tried. Raising `num_threads` was
attempted and reverted — see the risk note below, it made things worse, not
better. Practical effect here: open the page in a second tab and it will
just sit on "connecting…" until you close the first one. The demo still
correctly proves out `on()`/`join()`/`emitTo()` room-scoped delivery with a
single client; it does not currently demonstrate real concurrent multi-user
chat.

**`on()` callbacks that call `$client->join()` have a real, disassembly-confirmed
`zend_mm_heap corrupted` UAF, but it's currently very hard to reproduce
on demand.** It was 100%-reproducible as of 2026-08-04. Since then: two
sessions totaling 5100+ armed trials at the default `num_threads=1`
reproduced nothing; separately, raising `num_threads` to allow multiple
concurrent clients reproduced the crash again almost immediately via 2 real
browser tabs, but ~900 further armed trials (scripted, not browser-driven)
failed to reproduce that either. Current status: **real, but currently
un-reproducible on demand — not confirmed fixed, not currently
root-caused** (see project memory `socket_websocket_crash_fix.md` for the
full investigation, if you have access to it). This demo exercises exactly
the pattern that triggers it. If you see `zend_mm_heap corrupted` /
`SIGABRT` while running it, that's a live data point worth capturing
(server log, request sequence, `php -i | grep -i debug`) rather than just a
restart-and-move-on annoyance.

## Running it

```sh
./start.sh    # starts chat-server.php (:9200) and a static file server (:9201)
./stop.sh
```

Then open `http://127.0.0.1:9201` in a browser and join a room — you'll see
your own `joined`/`user_joined` presence events and can send chat messages
to yourself. Opening a second tab will not work concurrently (see "Known
limitations" above); close the first tab before opening a second one to
verify the room-scoped delivery mechanism independently.

### Custom ports / PHP binary

```sh
SOCKET_PORT=19200 WEB_PORT=19201 ./start.sh
```

```sh
PHP_BIN='php -n -d extension=/absolute/path/kislayphp_socket.so' ./start.sh
```

## Wire protocol walkthrough

```
server -> client   "0" + json     open packet, carries the session id
client -> server   "40"           CONNECT — required before events are processed
server -> client   "2"            ping (periodic housekeeping)
client -> server   "3"            pong reply
either direction   "42" + json    EVENT, json is ["eventName", data]
```

`public/app.js` implements exactly this against
`ws://host:port/socket.io/?EIO=4&transport=websocket`.
