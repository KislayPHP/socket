<?php
// php -d extension=modules/kislayphp_socket.so service_communication.php

if (!extension_loaded('kislayphp_socket')) {
    fwrite(STDERR, "kislayphp_socket extension is not loaded\n");
    exit(1);
}

$socket = new Kislay\Socket\Server();

$socket->on('connection', function ($client) {
    $client->join('svc.orders');
    $client->join('svc.inventory');
});

$socket->on('svc.request.inventory.reserve', function ($client, $payload) {
    $requestId = is_array($payload) ? ($payload['requestId'] ?? '') : '';
    $traceId = is_array($payload) ? ($payload['traceId'] ?? '') : '';
    $client->emit('svc.reply.inventory.reserve', [
        'requestId' => $requestId,
        'traceId' => $traceId,
        'status' => 'OK',
        'reserved' => true,
    ]);
});

$socket->on('evt.orders.created', function ($client, $payload) {
    $client->emitTo('svc.inventory', 'evt.orders.created', $payload);
});

echo "Socket service communication server listening on 0.0.0.0:8090\n";
$socket->listen('0.0.0.0', 8090, '/socket.io/');
