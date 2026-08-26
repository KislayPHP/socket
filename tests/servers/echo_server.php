<?php
// Minimal connection/disconnect server used by connection_basics_test.php.
$port = (int)($argv[1] ?? 0);
if ($port <= 0) {
    fwrite(STDERR, "usage: echo_server.php <port>\n");
    exit(1);
}

$server = new Kislay\Socket\Server();

$server->on('connection', function ($client) {
    fwrite(STDOUT, sprintf("connected %s\n", $client->id()));
});

$server->on('disconnect', function ($client) {
    fwrite(STDOUT, sprintf("disconnected %s\n", $client->id()));
});

fwrite(STDOUT, sprintf("listening on 127.0.0.1:%d\n", $port));
$server->listen('127.0.0.1', $port, '/socket.io/');
