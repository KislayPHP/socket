<?php
// Room join/leave/broadcast server used by room_broadcast_test.php.
//
// A client sends '42["subscribe",{"room":"..."}]' to join a room (mirrors
// notifications-service.php's on('subscribe') -> $client->join() shape,
// including calling join() from inside an on()-callback), then any client
// sends '42["announce",{"room":"...","msg":"..."}]' to have the server
// fan the message out to every member of that room via emitTo(), which is
// what a real caller (e.g. a Redis-relayed settlement event) does.
$port = (int)($argv[1] ?? 0);
if ($port <= 0) {
    fwrite(STDERR, "usage: room_server.php <port>\n");
    exit(1);
}

$server = new Kislay\Socket\Server();

$server->on('connection', function ($client) {
    fwrite(STDOUT, sprintf("connected %s\n", $client->id()));
});

$server->on('subscribe', function ($client, $data) {
    $room = trim((string)($data['room'] ?? ''));
    if ($room === '') {
        return;
    }
    $client->join(sprintf('room:%s', $room));
    fwrite(STDOUT, sprintf("%s joined room:%s\n", $client->id(), $room));
});

$server->on('leave', function ($client, $data) {
    $room = trim((string)($data['room'] ?? ''));
    if ($room === '') {
        return;
    }
    $client->leave(sprintf('room:%s', $room));
    fwrite(STDOUT, sprintf("%s left room:%s\n", $client->id(), $room));
});

$server->on('announce', function ($client, $data) use ($server) {
    $room = trim((string)($data['room'] ?? ''));
    $msg = (string)($data['msg'] ?? '');
    if ($room === '') {
        return;
    }
    $server->emitTo(sprintf('room:%s', $room), 'announced', ['msg' => $msg]);
});

$server->on('disconnect', function ($client) {
    fwrite(STDOUT, sprintf("disconnected %s\n", $client->id()));
});

fwrite(STDOUT, sprintf("listening on 127.0.0.1:%d\n", $port));
$server->listen('127.0.0.1', $port, '/socket.io/');
