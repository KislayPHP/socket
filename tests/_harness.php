<?php
// Shared test harness for kislay_socket tests. Not a test itself.
//
// Every test spawns the real compiled extension (modules/kislayphp_socket.so)
// as a SEPARATE PHP CLI process running one of tests/servers/*.php, then
// drives it over real HTTP (curl) exactly like a browser/curl client would -
// this is deliberate: the crash under investigation
// (zend_mm_heap corrupted, see /Users/kislay/kislayphp/bankapp/notifications-service.php's
// header comment) is a cross-thread race inside the extension's own process,
// so it can only be observed by actually running the extension as a
// standalone process and watching whether it dies, not by unit-testing PHP
// userland code in-process.

define('KISLAY_SOCKET_EXT', __DIR__ . '/../modules/kislayphp_socket.so');
define('KISLAY_SOCKET_SERVERS_DIR', __DIR__ . '/servers');

if (!file_exists(KISLAY_SOCKET_EXT)) {
    fwrite(STDERR, "Extension not built: " . KISLAY_SOCKET_EXT . " - run `make` in socket/ first.\n");
    exit(1);
}

function kislay_test_free_port(): int {
    $sock = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($sock === false) {
        throw new RuntimeException("could not allocate a free port: $errstr");
    }
    $name = stream_socket_get_name($sock, false);
    fclose($sock);
    return (int)substr($name, strrpos($name, ':') + 1);
}

/**
 * Starts one of tests/servers/*.php as a background PHP CLI process with the
 * extension loaded, waits for it to start accepting connections, and returns
 * a handle describing it.
 */
function kislay_test_start_server(string $serverScript, int $port, array $env = []): array {
    $scriptPath = KISLAY_SOCKET_SERVERS_DIR . '/' . $serverScript;
    if (!file_exists($scriptPath)) {
        throw new RuntimeException("no such test server script: $scriptPath");
    }
    $logFile = tempnam(sys_get_temp_dir(), 'kislay_socket_test_');

    $cmd = sprintf(
        '%s -d extension=%s %s %d >%s 2>&1',
        escapeshellarg(PHP_BINARY),
        escapeshellarg(KISLAY_SOCKET_EXT),
        escapeshellarg($scriptPath),
        $port,
        escapeshellarg($logFile)
    );

    $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
    $fullEnv = array_merge($_ENV ?: [], $env);
    // Bypass any shell so we get the real server PID directly (needed to
    // check liveness / send signals), not a `sh -c ...` wrapper PID.
    $proc = proc_open(
        [PHP_BINARY, '-d', 'extension=' . KISLAY_SOCKET_EXT, $scriptPath, (string)$port],
        [0 => ['pipe', 'r'], 1 => ['file', $logFile, 'w'], 2 => ['file', $logFile, 'a']],
        $pipes,
        null,
        $fullEnv
    );
    if ($proc === false) {
        throw new RuntimeException("failed to start server process for $serverScript");
    }
    fclose($pipes[0]);

    $status = proc_get_status($proc);
    $pid = $status['pid'];

    if (!kislay_test_wait_port('127.0.0.1', $port, 5.0)) {
        $log = @file_get_contents($logFile);
        proc_terminate($proc, 9);
        proc_close($proc);
        throw new RuntimeException("server $serverScript did not start listening on port $port within 5s. Log:\n$log");
    }

    return [
        'proc' => $proc,
        'pid' => $pid,
        'port' => $port,
        'log' => $logFile,
        'script' => $serverScript,
    ];
}

function kislay_test_wait_port(string $host, int $port, float $timeout): bool {
    $deadline = microtime(true) + $timeout;
    while (microtime(true) < $deadline) {
        $conn = @fsockopen($host, $port, $errno, $errstr, 0.2);
        if ($conn !== false) {
            fclose($conn);
            return true;
        }
        usleep(50000);
    }
    return false;
}

/** True if the OS process behind $handle is still running. */
function kislay_test_is_alive(array $handle): bool {
    $status = proc_get_status($handle['proc']);
    return $status !== false && $status['running'];
}

/** Waits briefly and returns exit info once the process has stopped, or null if still running. */
function kislay_test_exit_info(array $handle): ?array {
    $status = proc_get_status($handle['proc']);
    if ($status === false || $status['running']) {
        return null;
    }
    return $status;
}

function kislay_test_stop_server(array $handle): void {
    if (kislay_test_is_alive($handle)) {
        posix_kill($handle['pid'], SIGTERM);
        $deadline = microtime(true) + 3.0;
        while (kislay_test_is_alive($handle) && microtime(true) < $deadline) {
            usleep(50000);
        }
        if (kislay_test_is_alive($handle)) {
            posix_kill($handle['pid'], SIGKILL);
            usleep(100000);
        }
    }
    @proc_close($handle['proc']);
}

/**
 * Scans a server's log for the specific symptoms of the heap-corruption
 * crash under investigation (see notifications-service.php's header
 * comment): "zend_mm_heap corrupted", an abort, or a segfault. Also treated
 * as a crash signal if the process silently exited on its own with a
 * signal or non-zero status (proc_open on macOS/Linux surfaces this as
 * 'signaled'/'termsig' or a non-zero 'exitcode' in proc_get_status()).
 */
function kislay_test_detect_crash(array $handle): ?string {
    $log = @file_get_contents($handle['log']) ?: '';
    if (preg_match('/(zend_mm_heap corrupted|SIGABRT|Segmentation fault|Bus error|Aborted|Fatal error: .*Zend)/i', $log, $m)) {
        return 'log matched: ' . trim($m[0]);
    }
    $exit = kislay_test_exit_info($handle);
    if ($exit !== null) {
        if (!empty($exit['signaled']) && $exit['termsig'] !== 0) {
            return 'process died from signal ' . $exit['termsig'];
        }
        if (($exit['exitcode'] ?? 0) !== 0 && ($exit['exitcode'] ?? 0) !== -1) {
            return 'process exited with code ' . $exit['exitcode'];
        }
    }
    return null;
}

/** Minimal curl GET/POST helpers matching the wire format used by
 * scripts/stress_transfer.sh and the Engine.IO/Socket.IO polling transport. */
function kislay_test_http_get(string $url, float $timeout = 10.0): array {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => $timeout,
    ]);
    $body = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    return ['body' => $body === false ? '' : $body, 'code' => $code, 'error' => $err];
}

function kislay_test_http_post(string $url, string $data, float $timeout = 10.0): array {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $data,
        CURLOPT_TIMEOUT => $timeout,
    ]);
    $body = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    return ['body' => $body === false ? '' : $body, 'code' => $code, 'error' => $err];
}

/** Starts a GET long-poll in the background (like stress_transfer.sh's `&` poll),
 * returning a curl multi handle to reap later with kislay_test_reap_async_get(). */
function kislay_test_http_get_async(string $url, float $timeout = 10.0) {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => $timeout,
    ]);
    $mh = curl_multi_init();
    curl_multi_add_handle($mh, $ch);
    return ['mh' => $mh, 'ch' => $ch];
}

function kislay_test_reap_async_get(array $handle): array {
    $mh = $handle['mh'];
    $ch = $handle['ch'];
    $running = null;
    do {
        curl_multi_exec($mh, $running);
        curl_multi_select($mh, 0.2);
    } while ($running > 0);
    $body = curl_multi_getcontent($ch);
    curl_multi_remove_handle($mh, $ch);
    curl_multi_close($mh);
    return ['body' => $body ?: ''];
}

function kislay_test_extract_sid(string $openPacketBody): ?string {
    if (preg_match('/"sid":"([^"]+)"/', $openPacketBody, $m)) {
        return $m[1];
    }
    return null;
}

/** Performs a fresh Engine.IO polling handshake and returns the sid. */
function kislay_test_handshake(string $base): string {
    $resp = kislay_test_http_get("$base/socket.io/?EIO=4&transport=polling");
    $sid = kislay_test_extract_sid($resp['body']);
    if ($sid === null) {
        throw new RuntimeException("handshake failed, no sid in response: " . var_export($resp, true));
    }
    return $sid;
}

$GLOBALS['kislay_test_failures'] = 0;
$GLOBALS['kislay_test_count'] = 0;

function kislay_test_assert(bool $cond, string $label): void {
    $GLOBALS['kislay_test_count']++;
    if ($cond) {
        echo "  ok - $label\n";
    } else {
        $GLOBALS['kislay_test_failures']++;
        echo "  FAIL - $label\n";
    }
}

function kislay_test_summary(): int {
    $total = $GLOBALS['kislay_test_count'];
    $fail = $GLOBALS['kislay_test_failures'];
    printf("\n%d/%d assertions passed\n", $total - $fail, $total);
    return $fail === 0 ? 0 : 1;
}
