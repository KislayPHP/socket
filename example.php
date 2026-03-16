<?php

extension_loaded('kislayphp_socket') or die('kislayphp_socket not loaded');

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

$server->listen('0.0.0.0', 8090, '/socket.io/');
