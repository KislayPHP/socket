#ifndef PHP_KISLAYPHP_SOCKET_H
#define PHP_KISLAYPHP_SOCKET_H

extern "C" {
#include "php.h"
}

#define PHP_KISLAYPHP_SOCKET_VERSION "0.0.1"
#define PHP_KISLAYPHP_SOCKET_EXTNAME "kislayphp_socket"

extern zend_module_entry kislayphp_socket_module_entry;
#define phpext_kislayphp_socket_ptr &kislayphp_socket_module_entry

#endif
