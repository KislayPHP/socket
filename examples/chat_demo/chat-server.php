<?php
// Multi-room chat backend for the chat_demo example. Demonstrates real
// Kislay\Socket\Server::on() event callbacks plus $client->join()/emitTo()
// for room-scoped delivery - the pattern socket/example.php and
// socket/service_communication.php already show for a single fixed room,
// extended here to a client-chosen room name and a matching browser
// frontend (public/index.html + app.js).
//
// Known risk: on() callbacks that call $client->join() were, as of
// 2026-08-04, a 100%-reproducible zend_mm_heap corrupted crash on first
// invocation. Two full investigation sessions since then - including a
// 5000-trial run with a debugger armed and watching on every single trial,
// ~9 hours, zero reproductions - have not reproduced it again. Current
// status: real, but currently un-reproducible on demand via scripted
// traffic (900+ armed trials failed to reproduce it after it fired again
// via 2 real browser tabs) - NOT confirmed fixed, NOT currently
// root-caused (see project memory socket_websocket_crash_fix.md for the
// full investigation). This demo exercises exactly that pattern; treat any
// zend_mm_heap corrupted crash while running it as a live data point worth
// capturing (pid, request sequence, `php -i | grep -i debug`), not just a
// restart-and-move-on annoyance.
//
// Also: Server::listen() hardcodes num_threads=1, so only ONE client can
// be connected at a time process-wide (confirmed directly - raising this
// was attempted and reverted after it reproduced the crash above). This
// demo is single-client only; see README.md "Known limitations".

extension_loaded('kislayphp_socket') or die('kislayphp_socket not loaded');

$port = (int)($argv[1] ?? 9200);

$server = new Kislay\Socket\Server();

$server->on('connection', function (Kislay\Socket\Socket $socket) {
    fwrite(STDOUT, "chat-demo: connected {$socket->id()}\n");
});

$server->on('join_room', function (Kislay\Socket\Socket $socket, $data) use ($server) {
    $room = trim((string)($data['room'] ?? '')) ?: 'lobby';
    $socket->join($room);
    $count = $server->roomCount($room);
    $socket->emit('joined', ['room' => $room, 'count' => $count]);
    $socket->emitTo($room, 'user_joined', ['room' => $room, 'count' => $count]);
});

$server->on('chat_message', function (Kislay\Socket\Socket $socket, $data) {
    $room = trim((string)($data['room'] ?? '')) ?: 'lobby';
    $text = trim((string)($data['text'] ?? ''));
    if ($text === '') {
        return;
    }
    $socket->emitTo($room, 'chat_message', [
        'from' => $socket->id(),
        'text' => $text,
        'ts' => time(),
    ]);
});

$server->on('disconnect', function (Kislay\Socket\Socket $socket) {
    fwrite(STDOUT, "chat-demo: disconnected {$socket->id()}\n");
});

fwrite(STDOUT, "chat-demo: listening on {$port}\n");
$server->listen('0.0.0.0', $port, '/socket.io/');
