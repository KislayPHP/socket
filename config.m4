PHP_ARG_ENABLE(kislayphp_socket, whether to enable kislayphp_socket,
[  --enable-kislayphp_socket   Enable kislayphp_socket support])

if test "$PHP_KISLAYPHP_SOCKET" != "no"; then
  PHP_REQUIRE_CXX()

  CIVETWEB_INCLUDE_DIR=`pwd`/third_party/civetweb/include
  PHP_ADD_INCLUDE($CIVETWEB_INCLUDE_DIR)

  PKG_CHECK_MODULES([OPENSSL], [openssl])
  PHP_EVAL_INCLINE($OPENSSL_CFLAGS)
  PHP_EVAL_LIBLINE($OPENSSL_LIBS, KISLAYPHP_SOCKET_SHARED_LIBADD)
  PHP_ADD_LIBRARY(stdc++,, KISLAYPHP_SOCKET_SHARED_LIBADD)
  PHP_SUBST(KISLAYPHP_SOCKET_SHARED_LIBADD)

  dnl USE_WEBSOCKET enables civetweb's websocket upgrade handling
  dnl (handle_websocket_request and its call sites are compiled out
  dnl entirely otherwise). Without this, mg_set_websocket_handler()/
  dnl mg_websocket_write() still link fine but every WS upgrade request
  dnl silently falls through with no response, since Socket\Server's core
  dnl advertised feature depends on this civetweb code path existing.
  dnl
  dnl NO_SSL_DL: without it, civetweb resolves OpenSSL/libcrypto functions
  dnl (including the SHA1 digest used by the websocket handshake) lazily at
  dnl runtime via dlopen()/dlsym() into a private function-pointer table,
  dnl populated only when TLS is actually configured (initialize_openssl()
  dnl is gated behind the MG_FEATURES_SSL flag). This is a plain-HTTP (no
  dnl TLS) server, so that table is never populated and every websocket
  dnl handshake calls through a NULL function pointer - SIGSEGV inside
  dnl send_websocket_handshake() on the very first upgrade attempt, on
  dnl every single connection. We already link directly against real
  dnl libssl/libcrypto above (PKG_CHECK_MODULES), so there's no reason to
  dnl go through civetweb's own dlopen indirection at all; NO_SSL_DL makes
  dnl civetweb call the linked OpenSSL functions directly instead.
  dnl -fvisibility=hidden + -DCIVETWEB_API=: civetweb.c exports ~200
  dnl non-static C functions (mg_start, mg_read, ...) with default (public)
  dnl visibility - civetweb.h's own CIVETWEB_API macro explicitly
  dnl re-asserts __attribute__((visibility("default"))) regardless of
  dnl -fvisibility, unless CIVETWEB_API is already defined (its #ifndef
  dnl guard), so pre-defining it as empty here is what actually makes
  dnl -fvisibility=hidden take effect, without touching the vendored
  dnl header/source at all. PHP extension bundles link with
  dnl -flat_namespace (confirmed in the actual link command on this
  dnl platform), so when 2+ extensions that each vendor their own copy of
  dnl civetweb.c are loaded into the same process (e.g. this extension
  dnl alongside core or gateway, which also embed civetweb), the dynamic
  dnl linker can resolve a call in ONE extension's object code to the
  dnl OTHER extension's same-named symbol - silently running the wrong
  dnl compiled civetweb, since the compiled versions can differ (different
  dnl flags, different fixes applied). Hiding everything by default
  dnl eliminates the collision at its source; get_module() stays exported
  dnl via ZEND_GET_MODULE's own ZEND_DLEXPORT, independent of this flag -
  dnl PHP's dlopen()-based loader only ever needs that one symbol by name.
  CFLAGS="$CFLAGS -DOPENSSL_API_3_0 -DUSE_WEBSOCKET -DNO_SSL_DL -fvisibility=hidden -DCIVETWEB_API="
  CXXFLAGS="$CXXFLAGS -DOPENSSL_API_3_0 -DUSE_WEBSOCKET -DNO_SSL_DL -fvisibility=hidden -DCIVETWEB_API="
  if test -f ../rpc/gen/platform.pb.cc; then
    RPC_GEN_DIR=`pwd`/../rpc/gen
    PHP_ADD_INCLUDE($RPC_GEN_DIR)
    PHP_ADD_INCLUDE(`pwd`/../rpc)
    PKG_CHECK_MODULES([GRPC], [grpc++])
    PHP_EVAL_INCLINE($GRPC_CFLAGS)
    PHP_EVAL_LIBLINE($GRPC_LIBS, KISLAYPHP_SOCKET_SHARED_LIBADD)
    CXXFLAGS="$CXXFLAGS -DKISLAYPHP_RPC"
    RPC_SRCS="../rpc/gen/platform.pb.cc ../rpc/gen/platform.grpc.pb.cc"
  else
    AC_MSG_WARN([RPC stubs not found. Building without RPC support])
    RPC_SRCS=""
  fi

  PHP_NEW_EXTENSION(kislayphp_socket, kislay_socket.cpp third_party/civetweb/src/civetweb.c $RPC_SRCS, $ext_shared)
fi
