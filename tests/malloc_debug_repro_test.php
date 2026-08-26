<?php
require __DIR__ . '/_harness.php';

// Reproduces the `zend_mm_heap corrupted` crash (see
// socket_websocket_crash_fix.md project memory) with the server's emalloc/efree
// traffic routed through the SYSTEM allocator instead of Zend's private zend_mm
// arena (USE_ZEND_ALLOC=0), with MallocScribble/MallocPreScribble enabled so
// freed memory is filled with a recognizable pattern (0x55) and freshly
// allocated-but-uninitialized memory with another (0xAA) - useful for spotting
// stale/UAF writes by eye if this is ever captured under a debugger again.
//
// Why this variant matters (found 2026-08-26): every previous debugging
// attempt (ASan, Guard Malloc, lldb) implicitly only ever observed Zend's own
// private zend_mm allocator, because emalloc()/efree() never call the system
// malloc()/free() at all by default - PHP's zend_mm carves memory out of a few
// large mmap'd chunks and manages its own free lists internally. That means
// every prior session's memory-debugging tool was structurally blind to 100%
// of this bug's actual allocations; they could only ever observe zend_mm's
// OWN internal bookkeeping getting corrupted, never the write itself.
//
// USE_ZEND_ALLOC=0 makes emalloc/efree thin wrappers around real malloc/free
// (see Zend/zend_alloc.c), which lets USER-LEVEL malloc debugging (ASan,
// Guard Malloc, or - what actually worked here - macOS's built-in
// MallocScribble/MallocPreScribble/MallocGuardEdges env vars, no library
// injection needed) see every allocation this extension makes, Zend's
// included.
//
// Result: the crash reproduces just as reliably this way (confirmed
// 13/15 = ~87% on a SINGLE client's SINGLE connect+subscribe(join) round, no
// second dispatch/round needed at all in this configuration - a faster,
// more minimal repro than any previously documented one), but is now
// detected via a SIGTRAP inside libsystem_malloc.dylib's own freelist
// consistency check (`malloc_error_break`, symbol
// `_xzm_xzone_malloc_freelist_outlined` on this macOS version) instead of
// zend_mm_panic. A live lldb capture (breakpoint on `malloc_error_break`)
// got a full, real backtrace through actual business logic for the first
// time:
//
//   ... libsystem_malloc.dylib (freelist corruption detected) ...
//   operator new(unsigned long)
//   std::__1::unordered_map<...>::operator[]        <- server->rooms[room]
//   zim_KislaySocketClient_join                        kislay_socket.cpp:2714
//   kislay_call_php / kislay_run_pending_calls / kislay_process_raw_events
//   zim_KislaySocketServer_listen
//
// i.e. the corruption is detected on the FIRST-EVER insertion of a new room
// into server->rooms (a plain std::unordered_map<string,
// unordered_set<string>> - completely unrelated to Zend/zval/zend_string),
// immediately after stress_server.php's subscribe handler does
// `$client->join(sprintf('account:%s', $accountId))`. This is strong
// evidence the corruption is a SIZE-CLASS FREELIST corruption in the shared
// allocator, not something specific to ZEND_ROPE/zend_string_alloc as such -
// every prior session's captures pointing at ZEND_ROPE_END are most likely
// just the statistically-likeliest next allocation to reuse the poisoned
// freelist slot (string/rope building being the most frequent small
// allocation in these closures), not the mechanism itself. Whatever writes
// past its bounds (or into a block after it was freed) does so at some
// earlier point and into a small, generic size-class bin - the NEXT
// allocation of roughly that size, whatever it is (Zend string, C++ hash
// node, ...), is what trips the detector.
//
// This DEFINITIVELY answers a question no prior session tested: is this a
// zend_mm-internal bug/false-positive, or a real memory-safety violation
// independent of which allocator backs PHP's emalloc? Answer: real and
// allocator-independent - confirmed via a second, structurally different
// allocator (Apple's "xzone malloc", replacing zend_mm here entirely)
// hitting the identical failure mode.
//
// Also tried and ruled out this session: DYLD_INSERT_LIBRARIES=libgmalloc.dylib
// (full Guard Malloc) + USE_ZEND_ALLOC=0 - crashes deterministically during
// kislay_socket_server_create_object's std::thread placement-new, before any
// client traffic at all (an environmental/resource incompatibility on this
// machine, not informative). ASan remains a complete dead end on this
// machine in every configuration tried across all sessions, now including
// this session's simpler "ASan-only extension + DYLD_INSERT_LIBRARIES into
// an unmodified php binary" attempt - even a bare `php -v` with zero
// extensions loaded spins at ~100% CPU forever (Apple's ASan runtime
// deadlocks in AsanInitFromRtl() on this exact macOS/Xcode combination) the
// instant the ASan dylib is interposed, so this is not fixable from this
// codebase's side.
//
// Usage: php malloc_debug_repro_test.php [trials]
$trials = (int)($argv[1] ?? 15);
$crashes = 0;
for ($t = 1; $t <= $trials; $t++) {
    $port = kislay_test_free_port();
    $env = [
        'MallocScribble' => '1',
        'MallocPreScribble' => '1',
        'USE_ZEND_ALLOC' => '0',
    ];
    $server = kislay_test_start_server('stress_server.php', $port, $env);
    $base = "http://127.0.0.1:$port";

    $resp = kislay_test_http_get("$base/socket.io/?EIO=4&transport=polling", 3.0);
    $sid = kislay_test_extract_sid($resp['body']);
    if ($sid !== null) {
        kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sid", '40', 3.0);
        kislay_test_http_post("$base/socket.io/?EIO=4&transport=polling&sid=$sid",
            '42["subscribe",{"account_id":"acct-1"}]', 3.0);
    }
    usleep(300000);
    $alive = kislay_test_is_alive($server);
    if (!$alive) {
        $crashes++;
        echo "  trial $t: CRASHED\n";
    } else {
        echo "  trial $t: survived\n";
    }
    kislay_test_stop_server($server);
}
echo "\n$crashes/$trials crashed under USE_ZEND_ALLOC=0 + MallocScribble (single client, single connect+subscribe(join) round each)\n";
echo "This is expected to be well above 0 - a HIGH crash count here reconfirms the bug is still present\n";
echo "and allocator-independent. It is not a pass/fail assertion (the bug isn't fixed).\n";
