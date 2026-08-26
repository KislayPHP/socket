<?php
// Mirrors bankapp/notifications-service.php's exact server shape - the one
// that reliably reproduced `zend_mm_heap corrupted` live (see that file's
// header comment): on('connection'), on('subscribe') calling $client->join()
// from inside the callback, on('disconnect'), sprintf()-only logging (the
// original, evidence-based-but-insufficient mitigation is kept here too, so
// this test isn't relying on a mitigation that's already known not to be
// sufficient by itself).
//
// ping/pong windows are intentionally configurable via env vars (already
// supported by the extension: KISLAYPHP_SOCKET_PING_INTERVAL_MS /
// KISLAYPHP_SOCKET_PING_TIMEOUT_MS) so a stress test can force the
// housekeeping-thread timeout-disconnect path (normally 25s+20s=45s away)
// to fire in seconds instead - that housekeeping-vs-civetweb-worker-thread
// race is the confirmed live trigger shape.
$port = (int)($argv[1] ?? 0);
if ($port <= 0) {
    fwrite(STDERR, "usage: stress_server.php <port>\n");
    exit(1);
}

$server = new Kislay\Socket\Server();

$server->on('connection', function ($client) {
    fwrite(STDOUT, sprintf("stress: connected %s\n", $client->id()));
    fflush(STDOUT);
});

$server->on('subscribe', function ($client, $data) {
    $accountId = trim((string)($data['account_id'] ?? ''));
    if ($accountId === '') {
        return;
    }
    $client->join(sprintf('account:%s', $accountId));
    fwrite(STDOUT, sprintf("stress: %s subscribed to account:%s\n", $client->id(), $accountId));
    fflush(STDOUT);
});

$server->on('disconnect', function ($client) {
    fwrite(STDOUT, sprintf("stress: disconnected %s\n", $client->id()));
    fflush(STDOUT);
});

fwrite(STDOUT, sprintf("stress_server: listening on 127.0.0.1:%d\n", $port));
fflush(STDOUT);
$server->listen('127.0.0.1', $port, '/socket.io/');
