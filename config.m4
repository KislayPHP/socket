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
  CFLAGS="$CFLAGS -DOPENSSL_API_3_0 -DUSE_WEBSOCKET -DNO_SSL_DL"
  CXXFLAGS="$CXXFLAGS -DOPENSSL_API_3_0 -DUSE_WEBSOCKET -DNO_SSL_DL"
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
