<?php
require __DIR__ . '/_harness.php';

// Stress test that mimics bankapp/scripts/stress_transfer.sh's traffic
// pattern (repeated connect -> subscribe/join -> disconnect cycles) against
// tests/servers/stress_server.php, which reproduces notifications-service.php's
// exact on('connection') / on('subscribe'){ $client->join() } / on('disconnect')
// shape - the confirmed live trigger for the `zend_mm_heap corrupted` crash
// documented in bankapp/notifications-service.php's header comment.
//
// Ping/pong windows are shortened via env vars so the housekeeping-thread
// timeout-disconnect path (normally ~45s away at defaults) fires within
// seconds. Half the rounds disconnect explicitly, half are abandoned (no
// disconnect sent, relying on the housekeeping timeout to clean up) -
// mirroring the confirmed live detail: "an explicit client-initiated
// disconnect landing close to a DIFFERENT, unrelated session's
// housekeeping-driven timeout disconnect - two back-to-back on('disconnect')
// dispatches from two different threads, main housekeeping thread vs
// civetweb worker thread."
//
// Usage: php stress_reconnect_test.php [rounds] [threads]
//   rounds  - number of connect/subscribe/(disconnect|abandon) cycles (default 60)
//   threads - KISLAYPHP_SOCKET_THREADS to pass to the server (default: unset)

$rounds = (int)($argv[1] ?? 60);
$threads = isset($argv[2]) && $argv[2] !== '' ? (int)$argv[2] : null;

echo "=== stress_reconnect_test (rounds=$rounds" . ($threads !== null ? ", threads=$threads" : "") . ") ===\n";

$port = kislay_test_free_port();
$env = [
    'KISLAYPHP_SOCKET_PING_INTERVAL_MS' => '400',
    'KISLAYPHP_SOCKET_PING_TIMEOUT_MS' => '400',
];
if ($threads !== null) {
    $env['KISLAYPHP_SOCKET_THREADS'] = (string)$threads;
}
$server = kislay_test_start_server('stress_server.php', $port, $env);
$base = "http://127.0.0.1:$port";

$crashedAtRound = null;

try {
    for ($i = 1; $i <= $rounds; $i++) {
        if (!kislay_test_is_alive($server)) {
            $crashedAtRound = $i;
            break;
        }

        $accountId = 'acct-' . ($i % 5);
        $resp = kislay_test_http_get("$base/socket.io/?EIO=4&transport=polling", 3.0);
        $sid = kislay_test_extract_sid($resp['body']);
        if ($sid === null) {
            // Handshake itself failing is worth noting but keep going -
            // crash detection at the end is the authoritative signal.
            continue;
        }

        kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sid", '40', 3.0);
        kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sid",
            '42["subscribe",{"account_id":"' . $accountId . '"}]', 3.0);

        // Background long-poll, exactly like stress_transfer.sh's `&`-ed curl.
        $poll = kislay_test_http_get_async("$base/socket.io/?EIO=4&transport=polling&sid=$sid", 2.0);

        if ($i % 2 === 0) {
            // Even rounds: explicit disconnect shortly after, racing the
            // background long-poll and any concurrently-expiring session's
            // housekeeping-driven disconnect.
            usleep(20000);
            kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sid", '1', 3.0);
            kislay_test_reap_async_get($poll);
        } else {
            // Odd rounds: abandon the session (no explicit disconnect) so it
            // must be cleaned up by the housekeeping thread's ping/pong
            // timeout instead - exercising the fix target directly.
            kislay_test_reap_async_get($poll);
        }

        if (!kislay_test_is_alive($server)) {
            $crashedAtRound = $i;
            break;
        }

        if ($i % 20 === 0) {
            echo "  round $i/$rounds done\n";
        }
    }

    // Give the housekeeping thread (1s tick) a couple of cycles to catch up
    // on any still-pending abandoned-session expiries before final checks.
    usleep(1500000);

    $crash = kislay_test_detect_crash($server);
    $alive = kislay_test_is_alive($server);

    if ($crashedAtRound !== null) {
        echo "  CRASH DETECTED: server process died during round $crashedAtRound\n";
    }
    if ($crash !== null) {
        echo "  CRASH SIGNATURE: $crash\n";
    }

    kislay_test_assert($crashedAtRound === null, 'server process survived all ' . $rounds . ' rounds');
    kislay_test_assert($crash === null, 'no crash signature (heap corruption/abort/segfault) in server log');
    kislay_test_assert($alive, 'server process still alive at end of test');
} finally {
    kislay_test_stop_server($server);
}

exit(kislay_test_summary());
