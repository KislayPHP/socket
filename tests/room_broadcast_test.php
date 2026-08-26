<?php
require __DIR__ . '/_harness.php';

echo "=== room_broadcast_test ===\n";

$port = kislay_test_free_port();
$server = kislay_test_start_server('room_server.php', $port);
$base = "http://127.0.0.1:$port";

try {
    // Two independent clients, both joining the same room via an
    // on('subscribe') callback that calls $client->join() - the exact
    // shape flagged as highest-risk in notifications-service.php.
    $sidA = kislay_test_handshake($base);
    $sidB = kislay_test_handshake($base);
    kislay_test_assert($sidA !== $sidB, 'two handshakes produce distinct sids');

    kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidA", '40');
    kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidB", '40');

    $resp = kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidA",
        '42["subscribe",{"room":"lobby"}]');
    kislay_test_assert($resp['code'] === 200, 'client A subscribe returns 200');
    $resp = kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidB",
        '42["subscribe",{"room":"lobby"}]');
    kislay_test_assert($resp['code'] === 200, 'client B subscribe returns 200');

    usleep(200000);
    $log = file_get_contents($server['log']);
    kislay_test_assert(str_contains($log, "$sidA joined room:lobby"), 'client A joined room:lobby');
    kislay_test_assert(str_contains($log, "$sidB joined room:lobby"), 'client B joined room:lobby');

    // Start both clients long-polling for the broadcast, then have client A
    // trigger $server->emitTo('room:lobby', ...) via the 'announce' event.
    $pollA = kislay_test_http_get_async("$base/socket.io/?EIO=4&transport=polling&sid=$sidA", 5.0);
    $pollB = kislay_test_http_get_async("$base/socket.io/?EIO=4&transport=polling&sid=$sidB", 5.0);

    usleep(100000);
    kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidA",
        '42["announce",{"room":"lobby","msg":"hello room"}]');

    $resultA = kislay_test_reap_async_get($pollA);
    $resultB = kislay_test_reap_async_get($pollB);

    kislay_test_assert(str_contains($resultA['body'], 'announced') && str_contains($resultA['body'], 'hello room'),
        'client A received the room broadcast');
    kislay_test_assert(str_contains($resultB['body'], 'announced') && str_contains($resultB['body'], 'hello room'),
        'client B received the room broadcast');

    // leave() should remove a client from the room without affecting others.
    kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidA",
        '42["leave",{"room":"lobby"}]');
    usleep(150000);
    $log = file_get_contents($server['log']);
    kislay_test_assert(str_contains($log, "$sidA left room:lobby"), 'client A left room:lobby');

    $pollA2 = kislay_test_http_get_async("$base/socket.io/?EIO=4&transport=polling&sid=$sidA", 2.0);
    $pollB2 = kislay_test_http_get_async("$base/socket.io/?EIO=4&transport=polling&sid=$sidB", 5.0);
    usleep(100000);
    kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sidB",
        '42["announce",{"room":"lobby","msg":"second message"}]');
    $resultA2 = kislay_test_reap_async_get($pollA2);
    $resultB2 = kislay_test_reap_async_get($pollB2);
    kislay_test_assert(!str_contains($resultA2['body'], 'second message'), 'client A (left) did NOT receive the second broadcast');
    kislay_test_assert(str_contains($resultB2['body'], 'second message'), 'client B (still joined) received the second broadcast');

    kislay_test_assert(kislay_test_is_alive($server), 'server process still alive');
    kislay_test_assert(kislay_test_detect_crash($server) === null, 'no crash signature in server log');
} finally {
    kislay_test_stop_server($server);
}

exit(kislay_test_summary());
