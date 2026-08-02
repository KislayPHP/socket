extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "ext/standard/base64.h"
#include "ext/standard/url.h"
#include "ext/json/php_json.h"
#include "Zend/zend_exceptions.h"
#include "Zend/zend_smart_str.h"
}

#include "php_kislay_socket.h"

#include <chrono>
#include <civetweb.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Zend/zend_smart_str.h"
#include "ext/standard/php_var.h"

#ifdef KISLAYPHP_RPC
#include <grpcpp/grpcpp.h>

#include "platform.grpc.pb.h"
#endif

static std::string kislay_to_lower(const std::string &value);

static zend_long kislay_env_long(const char *name, zend_long fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return static_cast<zend_long>(std::strtoll(value, nullptr, 10));
}

static bool kislay_env_bool(const char *name, bool fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    if (std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 || std::strcmp(value, "TRUE") == 0) {
        return true;
    }
    if (std::strcmp(value, "0") == 0 || std::strcmp(value, "false") == 0 || std::strcmp(value, "FALSE") == 0) {
        return false;
    }
    return fallback;
}

static std::string kislay_env_string(const char *name, const std::string &fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::string(value);
}

static zend_long kislay_env_long_any(const char *primary, const char *legacy, zend_long fallback) {
    const char *value = std::getenv(primary);
    if (value != nullptr && *value != '\0') {
        return static_cast<zend_long>(std::strtoll(value, nullptr, 10));
    }
    if (legacy != nullptr) {
        value = std::getenv(legacy);
        if (value != nullptr && *value != '\0') {
            return static_cast<zend_long>(std::strtoll(value, nullptr, 10));
        }
    }
    return fallback;
}

static bool kislay_env_bool_any(const char *primary, const char *legacy, bool fallback) {
    const char *value = std::getenv(primary);
    if ((value == nullptr || *value == '\0') && legacy != nullptr) {
        value = std::getenv(legacy);
    }
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    if (std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 || std::strcmp(value, "TRUE") == 0) {
        return true;
    }
    if (std::strcmp(value, "0") == 0 || std::strcmp(value, "false") == 0 || std::strcmp(value, "FALSE") == 0) {
        return false;
    }
    return fallback;
}

static std::string kislay_env_string_any(const char *primary, const char *legacy, const std::string &fallback) {
    const char *value = std::getenv(primary);
    if (value != nullptr && *value != '\0') {
        return std::string(value);
    }
    if (legacy != nullptr) {
        value = std::getenv(legacy);
        if (value != nullptr && *value != '\0') {
            return std::string(value);
        }
    }
    return fallback;
}

#ifdef KISLAYPHP_RPC
static bool kislay_rpc_enabled() {
    return kislay_env_bool("KISLAY_RPC_ENABLED", false);
}

static zend_long kislay_rpc_timeout_ms() {
    zend_long timeout = kislay_env_long("KISLAY_RPC_TIMEOUT_MS", 200);
    return timeout > 0 ? timeout : 200;
}

static std::string kislay_rpc_platform_endpoint() {
    return kislay_env_string("KISLAY_RPC_PLATFORM_ENDPOINT", "127.0.0.1:9100");
}

static bool kislay_serialize_payload(zval *payload, std::string &out) {
    smart_str buffer = {0};
    php_serialize_data_t var_hash;
    PHP_VAR_SERIALIZE_INIT(var_hash);
    php_var_serialize(&buffer, payload, &var_hash);
    PHP_VAR_SERIALIZE_DESTROY(var_hash);
    if (buffer.s == nullptr) {
        return false;
    }
    out.assign(ZSTR_VAL(buffer.s), ZSTR_LEN(buffer.s));
    smart_str_free(&buffer);
    return true;
}

static kislay::platform::v1::EventBusService::Stub *kislay_rpc_eventbus_stub(const std::string &endpoint) {
    static std::mutex lock;
    static std::string cached_endpoint;
    static std::shared_ptr<grpc::Channel> channel;
    static std::unique_ptr<kislay::platform::v1::EventBusService::Stub> stub;
    std::lock_guard<std::mutex> guard(lock);
    if (!stub || cached_endpoint != endpoint) {
        channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        stub = kislay::platform::v1::EventBusService::NewStub(channel);
        cached_endpoint = endpoint;
    }
    return stub.get();
}

static bool kislay_rpc_publish(const std::string &topic, zval *payload, std::string *error) {
    auto *stub = kislay_rpc_eventbus_stub(kislay_rpc_platform_endpoint());
    if (!stub) {
        if (error) {
            *error = "RPC stub unavailable";
        }
        return false;
    }

    std::string payload_bytes;
    if (!kislay_serialize_payload(payload, payload_bytes)) {
        if (error) {
            *error = "Payload serialize failed";
        }
        return false;
    }

    kislay::platform::v1::PublishRequest request;
    request.set_topic(topic);
    request.set_payload(payload_bytes);
    kislay::platform::v1::PublishResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(kislay_rpc_timeout_ms()));

    grpc::Status status = stub->Publish(&context, request, &response);
    if (!status.ok()) {
        if (error) {
            *error = status.error_message();
        }
        return false;
    }
    if (!response.ok()) {
        if (error) {
            *error = response.error();
        }
        return false;
    }
    return true;
}
#endif

static void kislay_parse_csv(const std::string &value, std::vector<std::string> &out) {
    out.clear();
    size_t start = 0;
    while (start < value.size()) {
        size_t comma = value.find(',', start);
        size_t end = (comma == std::string::npos) ? value.size() : comma;
        while (start < end && std::isspace(static_cast<unsigned char>(value[start]))) {
            start++;
        }
        while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
            end--;
        }
        if (end > start) {
            out.emplace_back(value.substr(start, end - start));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
}

static void kislay_parse_transports(const std::string &value, std::unordered_set<std::string> &out) {
    out.clear();
    std::vector<std::string> parts;
    kislay_parse_csv(value, parts);
    for (auto &entry : parts) {
        out.insert(kislay_to_lower(entry));
    }
}

static zend_class_entry *kislay_socket_server_ce;
static zend_class_entry *kislay_socket_client_ce;
static zend_class_entry *kislay_ack_ce;
static zend_class_entry *kislay_namespace_ce;

ZEND_BEGIN_MODULE_GLOBALS(kislayphp_socket)
    zend_long ping_interval_ms;
    zend_long ping_timeout_ms;
    zend_long max_payload;
    zend_bool cors_enabled;
    zend_bool allow_upgrade;
    char *transports;
    zend_bool auth_enabled;
    char *auth_token;
    char *auth_query_keys;
    char *auth_header_keys;
ZEND_END_MODULE_GLOBALS(kislayphp_socket)

ZEND_DECLARE_MODULE_GLOBALS(kislayphp_socket)

#define KISLAYPHP_SOCKET_G(v) ZEND_MODULE_GLOBALS_ACCESSOR(kislayphp_socket, v)

#if defined(ZTS)
ZEND_TSRMLS_CACHE_EXTERN();
#endif

PHP_INI_BEGIN()
    STD_PHP_INI_ENTRY("kislayphp.socket.ping_interval_ms", "25000", PHP_INI_ALL, OnUpdateLong, ping_interval_ms, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.ping_timeout_ms", "20000", PHP_INI_ALL, OnUpdateLong, ping_timeout_ms, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.max_payload", "1000000", PHP_INI_ALL, OnUpdateLong, max_payload, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.cors", "1", PHP_INI_ALL, OnUpdateBool, cors_enabled, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.allow_upgrade", "1", PHP_INI_ALL, OnUpdateBool, allow_upgrade, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.transports", "polling,websocket", PHP_INI_ALL, OnUpdateString, transports, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.auth.enabled", "0", PHP_INI_ALL, OnUpdateBool, auth_enabled, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.auth.token", "", PHP_INI_ALL, OnUpdateString, auth_token, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.auth.query_keys", "token,auth", PHP_INI_ALL, OnUpdateString, auth_query_keys, zend_kislayphp_socket_globals, kislayphp_socket_globals)
    STD_PHP_INI_ENTRY("kislayphp.socket.auth.header_keys", "authorization,x-auth-token", PHP_INI_ALL, OnUpdateString, auth_header_keys, zend_kislayphp_socket_globals, kislayphp_socket_globals)
PHP_INI_END()

struct kislay_socket_client_state {
    struct mg_connection *conn;
    std::string sid;
    std::unordered_set<std::string> rooms;
};

struct kislay_socket_pending_binary {
    bool active;
    int expected;
    int received;
    std::string event;
    zval payload;
    std::vector<std::string> binaries;
};

struct kislay_socket_session {
    std::string sid;
    struct mg_connection *ws_conn;
    bool ws_upgraded;
    std::vector<std::string> queue;
    kislay_socket_pending_binary pending;
    std::chrono::steady_clock::time_point last_ping;
    std::chrono::steady_clock::time_point last_pong;
    std::string handshake_path;
    std::string handshake_query_string;
    std::unordered_map<std::string, std::string> handshake_headers;
};

struct kislay_pending_call {
    std::string event;
    std::string sid;
    zval handler;
    zval payload;
    bool has_payload;
    bool needs_ack;
    bool check_auth;
    std::string handshake_path;
    std::string handshake_query_string;
    std::unordered_map<std::string, std::string> handshake_headers;
};

typedef struct _php_kislay_socket_server_t {
    zend_object std;
    struct mg_context *ctx;
    std::string path;
    std::unordered_map<std::string, zval> handlers;
    std::unordered_map<std::string, kislay_socket_client_state> clients;
    std::unordered_map<struct mg_connection *, std::string> conn_to_sid;
    std::unordered_map<std::string, std::unordered_set<std::string>> rooms;
    std::unordered_map<std::string, kislay_socket_session> sessions;
    std::mutex lock;
    std::condition_variable cv;
    std::atomic<uint64_t> counter;
    bool running;
    bool auth_enabled;
    std::string auth_token;
    std::vector<std::string> auth_query_keys;
    std::vector<std::string> auth_header_keys;
    std::unordered_set<std::string> transports;
    bool allow_upgrade;
    bool cors_enabled;
    int ping_interval_ms;
    int ping_timeout_ms;
    size_t max_payload;
    zval auth_handler;
    bool has_auth_handler;
    std::unordered_map<std::string, zval> ack_handlers;
} php_kislay_socket_server_t;

typedef struct _php_kislay_socket_client_t {
    zend_object std;
    std::string sid;
    php_kislay_socket_server_t *server;
} php_kislay_socket_client_t;

static void kislay_remove_client(php_kislay_socket_server_t *server, const std::string &sid);

static zend_object_handlers kislay_socket_server_handlers;
static zend_object_handlers kislay_socket_client_handlers;
static zend_object_handlers kislay_ack_handlers_obj;
static zend_object_handlers kislay_namespace_handlers_obj;
typedef struct _php_kislay_ack_t {
    zend_object std;
    std::string sid;
    php_kislay_socket_server_t *server;
} php_kislay_ack_t;

typedef struct _php_kislay_namespace_t {
    zend_object std;
    zval server_obj;
    std::string ns;
} php_kislay_namespace_t;

static inline php_kislay_ack_t *php_kislay_ack_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislay_ack_t *>(
        reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislay_ack_t, std));
}

static inline php_kislay_namespace_t *php_kislay_namespace_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislay_namespace_t *>(
        reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislay_namespace_t, std));
}



static inline php_kislay_socket_server_t *php_kislay_socket_server_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislay_socket_server_t *>(
        reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislay_socket_server_t, std));
}

static inline php_kislay_socket_client_t *php_kislay_socket_client_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislay_socket_client_t *>(
        reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislay_socket_client_t, std));
}

static zend_object *kislay_socket_server_create_object(zend_class_entry *ce) {
    php_kislay_socket_server_t *server = static_cast<php_kislay_socket_server_t *>(
        ecalloc(1, sizeof(php_kislay_socket_server_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&server->std, ce);
    object_properties_init(&server->std, ce);
    new (&server->path) std::string();
    new (&server->handlers) std::unordered_map<std::string, zval>();
    new (&server->clients) std::unordered_map<std::string, kislay_socket_client_state>();
    new (&server->conn_to_sid) std::unordered_map<struct mg_connection *, std::string>();
    new (&server->rooms) std::unordered_map<std::string, std::unordered_set<std::string>>();
    new (&server->sessions) std::unordered_map<std::string, kislay_socket_session>();
    new (&server->lock) std::mutex();
    new (&server->cv) std::condition_variable();
    new (&server->counter) std::atomic<uint64_t>(0);
    server->ctx = nullptr;
    server->running = false;
    server->has_auth_handler = false;
    ZVAL_UNDEF(&server->auth_handler);
    new (&server->ack_handlers) std::unordered_map<std::string, zval>();
    server->auth_enabled = kislay_env_bool_any("KISLAYPHP_SOCKET_AUTH_ENABLED", "KISLAYPHP_AUTH_ENABLED", KISLAYPHP_SOCKET_G(auth_enabled) != 0);
    new (&server->auth_token) std::string(kislay_env_string_any("KISLAYPHP_SOCKET_AUTH_TOKEN", "KISLAYPHP_AUTH_TOKEN", KISLAYPHP_SOCKET_G(auth_token) ? KISLAYPHP_SOCKET_G(auth_token) : ""));
    new (&server->auth_query_keys) std::vector<std::string>();
    new (&server->auth_header_keys) std::vector<std::string>();
    new (&server->transports) std::unordered_set<std::string>();
    server->allow_upgrade = kislay_env_bool_any("KISLAYPHP_SOCKET_ALLOW_UPGRADE", "KISLAYPHP_EVENTBUS_ALLOW_UPGRADE", KISLAYPHP_SOCKET_G(allow_upgrade) != 0);
    server->cors_enabled = kislay_env_bool_any("KISLAYPHP_SOCKET_CORS", "KISLAYPHP_EVENTBUS_CORS", KISLAYPHP_SOCKET_G(cors_enabled) != 0);
    server->ping_interval_ms = static_cast<int>(kislay_env_long_any("KISLAYPHP_SOCKET_PING_INTERVAL_MS", "KISLAYPHP_EVENTBUS_PING_INTERVAL_MS", KISLAYPHP_SOCKET_G(ping_interval_ms)));
    server->ping_timeout_ms = static_cast<int>(kislay_env_long_any("KISLAYPHP_SOCKET_PING_TIMEOUT_MS", "KISLAYPHP_EVENTBUS_PING_TIMEOUT_MS", KISLAYPHP_SOCKET_G(ping_timeout_ms)));
    zend_long max_payload = kislay_env_long_any("KISLAYPHP_SOCKET_MAX_PAYLOAD", "KISLAYPHP_EVENTBUS_MAX_PAYLOAD", KISLAYPHP_SOCKET_G(max_payload));
    if (max_payload < 0) {
        max_payload = 0;
    }
    server->max_payload = static_cast<size_t>(max_payload);

    std::string query_keys = kislay_env_string_any("KISLAYPHP_SOCKET_AUTH_QUERY_KEYS", "KISLAYPHP_AUTH_QUERY_KEYS", KISLAYPHP_SOCKET_G(auth_query_keys) ? KISLAYPHP_SOCKET_G(auth_query_keys) : "");
    std::string header_keys = kislay_env_string_any("KISLAYPHP_SOCKET_AUTH_HEADER_KEYS", "KISLAYPHP_AUTH_HEADER_KEYS", KISLAYPHP_SOCKET_G(auth_header_keys) ? KISLAYPHP_SOCKET_G(auth_header_keys) : "");
    std::string transports = kislay_env_string_any("KISLAYPHP_SOCKET_TRANSPORTS", "KISLAYPHP_EVENTBUS_TRANSPORTS", KISLAYPHP_SOCKET_G(transports) ? KISLAYPHP_SOCKET_G(transports) : "");
    kislay_parse_csv(query_keys, server->auth_query_keys);
    kislay_parse_csv(header_keys, server->auth_header_keys);
    kislay_parse_transports(transports, server->transports);
    if (server->auth_query_keys.empty()) {
        server->auth_query_keys.push_back("token");
        server->auth_query_keys.push_back("auth");
    }
    if (server->auth_header_keys.empty()) {
        server->auth_header_keys.push_back("authorization");
        server->auth_header_keys.push_back("x-auth-token");
    }
    if (server->transports.empty()) {
        server->transports.insert("polling");
        server->transports.insert("websocket");
    }
    server->std.handlers = &kislay_socket_server_handlers;
    return &server->std;
}

static void kislay_socket_server_free_obj(zend_object *object) {
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(object);
    for (auto &handler : server->handlers) {
        zval_ptr_dtor(&handler.second);
    }
    for (auto &handler : server->ack_handlers) {
        zval_ptr_dtor(&handler.second);
    }
    if (server->has_auth_handler) {
        zval_ptr_dtor(&server->auth_handler);
    }
    if (server->ctx != nullptr) {
        mg_stop(server->ctx);
        server->ctx = nullptr;
    }
    for (auto &session : server->sessions) {
        if (session.second.pending.active) {
            zval_ptr_dtor(&session.second.pending.payload);
        }
    }
    server->sessions.~unordered_map();
    server->rooms.~unordered_map();
    server->conn_to_sid.~unordered_map();
    server->clients.~unordered_map();
    server->ack_handlers.~unordered_map();
    server->handlers.~unordered_map();
    server->path.~basic_string();
    server->auth_token.~basic_string();
    server->auth_query_keys.~vector();
    server->auth_header_keys.~vector();
    server->transports.~unordered_set();
    server->lock.~mutex();
    server->cv.~condition_variable();
    server->counter.~atomic();
    zend_object_std_dtor(&server->std);
}

static zend_object *kislay_socket_client_create_object(zend_class_entry *ce) {
    php_kislay_socket_client_t *client = static_cast<php_kislay_socket_client_t *>(
        ecalloc(1, sizeof(php_kislay_socket_client_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&client->std, ce);
    object_properties_init(&client->std, ce);
    new (&client->sid) std::string();
    client->server = nullptr;
    client->std.handlers = &kislay_socket_client_handlers;
    return &client->std;
}

static void kislay_socket_client_free_obj(zend_object *object) {
    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(object);
    client->sid.~basic_string();
    zend_object_std_dtor(&client->std);
}


static zend_object *kislay_ack_create_object(zend_class_entry *ce) {
    php_kislay_ack_t *ack = static_cast<php_kislay_ack_t *>(
        ecalloc(1, sizeof(php_kislay_ack_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&ack->std, ce);
    object_properties_init(&ack->std, ce);
    new (&ack->sid) std::string();
    ack->server = nullptr;
    ack->std.handlers = &kislay_ack_handlers_obj;
    return &ack->std;
}

static void kislay_ack_free_obj(zend_object *object) {
    php_kislay_ack_t *ack = php_kislay_ack_from_obj(object);
    ack->sid.~basic_string();
    zend_object_std_dtor(&ack->std);
}

static zend_object *kislay_namespace_create_object(zend_class_entry *ce) {
    php_kislay_namespace_t *ns = static_cast<php_kislay_namespace_t *>(
        ecalloc(1, sizeof(php_kislay_namespace_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&ns->std, ce);
    object_properties_init(&ns->std, ce);
    ZVAL_UNDEF(&ns->server_obj);
    new (&ns->ns) std::string();
    ns->std.handlers = &kislay_namespace_handlers_obj;
    return &ns->std;
}

static void kislay_namespace_free_obj(zend_object *object) {
    php_kislay_namespace_t *ns = php_kislay_namespace_from_obj(object);
    zval_ptr_dtor(&ns->server_obj);
    ns->ns.~basic_string();
    zend_object_std_dtor(&ns->std);
}

static bool kislay_is_callable(zval *callable) {
    zend_string *callable_name = nullptr;
    bool ok = zend_is_callable(callable, 0, &callable_name) != 0;
    if (callable_name != nullptr) {
        zend_string_release(callable_name);
    }
    return ok;
}

static std::string kislay_to_lower(const std::string &value) {
    std::string out = value;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}

static std::string kislay_url_decode(const std::string &value) {
    if (value.empty()) {
        return "";
    }
    std::string out = value;
    size_t new_len = php_url_decode(&out[0], out.size());
    out.resize(new_len);
    return out;
}

static void kislay_parse_query(const char *query, std::unordered_map<std::string, std::string> &out) {
    if (query == nullptr || *query == '\0') {
        return;
    }
    const char *start = query;
    const char *cur = query;
    while (true) {
        if (*cur == '&' || *cur == '\0') {
            std::string pair(start, static_cast<size_t>(cur - start));
            if (!pair.empty()) {
                size_t eq = pair.find('=');
                std::string key = (eq == std::string::npos) ? pair : pair.substr(0, eq);
                std::string val = (eq == std::string::npos) ? "" : pair.substr(eq + 1);
                key = kislay_url_decode(key);
                val = kislay_url_decode(val);
                if (!key.empty()) {
                    out[key] = val;
                }
            }
            if (*cur == '\0') {
                break;
            }
            start = cur + 1;
        }
        ++cur;
    }
}

static std::string kislay_engineio_encode_payload(const std::vector<std::string> &packets) {
    if (packets.empty()) {
        return "";
    }
    if (packets.size() == 1) {
        return packets[0];
    }
    std::string out;
    for (const auto &packet : packets) {
        out.append(std::to_string(packet.size()));
        out.push_back(':');
        out.append(packet);
    }
    return out;
}

static std::vector<std::string> kislay_engineio_parse_payload(const char *data, size_t data_len) {
    std::vector<std::string> packets;
    if (data_len == 0) {
        return packets;
    }

    size_t idx = 0;
    size_t probe = 0;
    while (probe < data_len && data[probe] >= '0' && data[probe] <= '9') {
        ++probe;
    }

    if (probe == 0 || probe >= data_len || data[probe] != ':') {
        packets.emplace_back(data, data_len);
        return packets;
    }

    while (idx < data_len) {
        size_t len = 0;
        while (idx < data_len && data[idx] >= '0' && data[idx] <= '9') {
            len = len * 10 + static_cast<size_t>(data[idx] - '0');
            ++idx;
        }
        if (idx >= data_len || data[idx] != ':') {
            break;
        }
        ++idx;
        if (idx + len > data_len) {
            break;
        }
        packets.emplace_back(data + idx, len);
        idx += len;
    }

    return packets;
}

// Every Zend/PHP API touched from a civetweb I/O thread or the Redis
// subscriber thread happens off the original PHP script thread (which is
// parked inside listen()). On NTS builds there's no TSRM/per-thread Zend
// engine isolation - calling into Zend from a thread with no
// synchronization relative to whichever thread last touched it is a data
// race at the C++ memory-model level (no happens-before edge for
// zend_mm/executor_globals state), and reliably corrupts the heap
// (zend_mm_panic) - not just from concurrent user callback invocations, but
// from ANY Zend call (php_json_decode, zval construction/destruction, etc)
// made from more than one such thread. Core's own dedicated-thread PHP
// execution (PhpRuntimePool::worker_main, core/src/runtime/php_runtime.cpp)
// has the identical shape and serializes every such call through a single
// mutex (nts_lock_) for exactly this reason. This mirrors that, as a
// recursive mutex since call sites nest (e.g. kislay_call_php is invoked
// from within an already-locked packet-handling scope).
static std::recursive_mutex kislay_socket_php_call_lock;

static bool kislay_call_php(zval *callable, uint32_t argc, zval *argv, zval *retval) {
    std::lock_guard<std::recursive_mutex> guard(kislay_socket_php_call_lock);
    ZVAL_UNDEF(retval);
    if (call_user_function(EG(function_table), nullptr, callable, retval, argc, argv) == FAILURE) {
        return false;
    }
    return true;
}


static void kislay_capture_handshake(const struct mg_connection *conn, kislay_socket_session &session) {
    const struct mg_request_info *ri = mg_get_request_info(conn);
    if (ri == nullptr) { return; }
    session.handshake_path = ri->local_uri ? ri->local_uri : "";
    session.handshake_query_string = ri->query_string ? ri->query_string : "";
    for (int i = 0; i < ri->num_headers; ++i) {
        session.handshake_headers[kislay_to_lower(ri->http_headers[i].name)] =
            ri->http_headers[i].value ? ri->http_headers[i].value : "";
    }
}

static std::string kislay_generate_sid(php_kislay_socket_server_t *server) {
    uint64_t counter = server->counter.fetch_add(1, std::memory_order_relaxed);
    return "sid-" + std::to_string(static_cast<unsigned long long>(counter + 1));
}

static std::string kislay_build_open_packet(const std::string &sid,
                                            int ping_interval_ms,
                                            int ping_timeout_ms,
                                            size_t max_payload,
                                            bool allow_upgrade) {
    std::string upgrades = allow_upgrade ? "[\"websocket\"]" : "[]";
    std::string json = "{\"sid\":\"" + sid + "\",\"upgrades\":" + upgrades +
        ",\"pingInterval\":" + std::to_string(ping_interval_ms) +
        ",\"pingTimeout\":" + std::to_string(ping_timeout_ms) +
        ",\"maxPayload\":" + std::to_string(max_payload) + "}";
    return "0" + json;
}

static void kislay_clear_pending(kislay_socket_pending_binary &pending) {
    if (pending.active) {
        zval_ptr_dtor(&pending.payload);
    }
    pending.active = false;
    pending.expected = 0;
    pending.received = 0;
    pending.event.clear();
    pending.binaries.clear();
}

static void kislay_replace_placeholders(zval *value, const std::vector<std::string> &binaries) {
    if (value == nullptr) {
        return;
    }
    if (Z_TYPE_P(value) == IS_ARRAY) {
        zval *placeholder = zend_hash_str_find(Z_ARRVAL_P(value), "_placeholder", sizeof("_placeholder") - 1);
        zval *num = zend_hash_str_find(Z_ARRVAL_P(value), "num", sizeof("num") - 1);
        if (placeholder != nullptr && num != nullptr && zend_is_true(placeholder) && Z_TYPE_P(num) == IS_LONG) {
            long idx = Z_LVAL_P(num);
            if (idx >= 0 && static_cast<size_t>(idx) < binaries.size()) {
                const std::string &bin = binaries[static_cast<size_t>(idx)];
                zval_ptr_dtor(value);
                ZVAL_STRINGL(value, bin.data(), bin.size());
                return;
            }
        }
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(value), zval *child) {
            kislay_replace_placeholders(child, binaries);
        } ZEND_HASH_FOREACH_END();
    }
}

static bool kislay_engineio_send_packet(php_kislay_socket_server_t *server, kislay_socket_session &session, const std::string &packet) {
    (void)server;
    if (session.ws_conn != nullptr && session.ws_upgraded) {
        mg_websocket_write(session.ws_conn, MG_WEBSOCKET_OPCODE_TEXT, packet.data(), packet.size());
        return true;
    }
    // Backpressure: cap per-session queue at 512 messages, drop oldest
    static const size_t KISLAY_SOCKET_MAX_QUEUE = 512;
    if (session.queue.size() >= KISLAY_SOCKET_MAX_QUEUE) {
        session.queue.erase(session.queue.begin()); // drop oldest
    }
    session.queue.push_back(packet);
    server->cv.notify_all();
    return true;
}

static bool kislay_engineio_send_packet_to_sid(php_kislay_socket_server_t *server, const std::string &sid, const std::string &packet) {
    auto sit = server->sessions.find(sid);
    if (sit == server->sessions.end()) {
        return false;
    }
    return kislay_engineio_send_packet(server, sit->second, packet);
}

static bool kislay_send_socketio_packet(php_kislay_socket_server_t *server, const std::string &sid, const std::string &packet) {
    std::string engine_packet = "4" + packet;
    return kislay_engineio_send_packet_to_sid(server, sid, engine_packet);
}

static bool kislay_send_socketio_event(php_kislay_socket_server_t *server, const std::string &sid, const std::string &event, zval *data) {
    zval payload;
    array_init(&payload);
    add_next_index_string(&payload, event.c_str());
    if (data != nullptr) {
        add_next_index_zval(&payload, data);
        Z_TRY_ADDREF_P(data);
    }

    smart_str buf = {0};
    if (php_json_encode(&buf, &payload, 0) != SUCCESS) {
        smart_str_free(&buf);
        zval_ptr_dtor(&payload);
        return false;
    }
    smart_str_0(&buf);

    std::string packet = "2";
    if (buf.s != nullptr) {
        packet.append(ZSTR_VAL(buf.s), ZSTR_LEN(buf.s));
    }
    kislay_send_socketio_packet(server, sid, packet);

    smart_str_free(&buf);
    zval_ptr_dtor(&payload);
    return true;
}

static void kislay_broadcast(php_kislay_socket_server_t *server, const std::string &event, zval *data) {
    for (const auto &entry : server->clients) {
        kislay_send_socketio_event(server, entry.first, event, data);
    }
}

static void kislay_emit_room(php_kislay_socket_server_t *server, const std::string &room, const std::string &event, zval *data) {
    auto it = server->rooms.find(room);
    if (it == server->rooms.end()) {
        return;
    }
    for (const auto &sid : it->second) {
        auto cit = server->clients.find(sid);
        if (cit != server->clients.end()) {
            kislay_send_socketio_event(server, cit->first, event, data);
        }
    }
}

static bool kislay_parse_socketio_event_packet(const char *data,
                                               size_t data_len,
                                               std::string &event_out,
                                               zval *data_out,
                                               int *attachments_out,
                                               bool *binary_out) {
    if (data_len == 0) {
        return false;
    }

    size_t offset = 0;
    if (data_len >= 2 && data[0] == '4' && data[1] == '2') {
        offset = 2;
    }

    if (offset >= data_len) {
        return false;
    }

    char type = data[offset];
    if (type != '2' && type != '5') {
        return false;
    }

    if (binary_out != nullptr) {
        *binary_out = (type == '5');
    }
    if (attachments_out != nullptr) {
        *attachments_out = 0;
    }

    offset++;
    if (type == '5') {
        int attachments = 0;
        while (offset < data_len && data[offset] >= '0' && data[offset] <= '9') {
            attachments = attachments * 10 + (data[offset] - '0');
            offset++;
        }
        if (offset >= data_len || data[offset] != '-') {
            return false;
        }
        offset++;
        if (attachments_out != nullptr) {
            *attachments_out = attachments;
        }
    }

    if (offset < data_len && data[offset] == '/') {
        size_t comma = offset;
        while (comma < data_len && data[comma] != ',') {
            comma++;
        }
        if (comma >= data_len) {
            return false;
        }
        offset = comma + 1;
    }

    while (offset < data_len && data[offset] >= '0' && data[offset] <= '9') {
        offset++;
    }

    size_t json_pos = offset;
    while (json_pos < data_len && data[json_pos] != '[' && data[json_pos] != '{') {
        json_pos++;
    }
    if (json_pos >= data_len) {
        return false;
    }

    const char *json = data + json_pos;
    size_t json_len = data_len - json_pos;
    if (json_len == 0) {
        return false;
    }

    // Called from the civetweb worker thread; see kislay_socket_php_call_lock.
    std::lock_guard<std::recursive_mutex> php_guard(kislay_socket_php_call_lock);

    zval decoded;
    if (php_json_decode(&decoded, json, json_len, true, PHP_JSON_PARSER_DEFAULT_DEPTH) != SUCCESS) {
        return false;
    }
    if (Z_TYPE(decoded) != IS_ARRAY) {
        zval_ptr_dtor(&decoded);
        return false;
    }

    zval *event_val = zend_hash_index_find(Z_ARRVAL(decoded), 0);
    if (event_val == nullptr || Z_TYPE_P(event_val) != IS_STRING) {
        zval_ptr_dtor(&decoded);
        return false;
    }
    event_out.assign(Z_STRVAL_P(event_val), Z_STRLEN_P(event_val));

    zval *payload = zend_hash_index_find(Z_ARRVAL(decoded), 1);
    if (payload != nullptr) {
        ZVAL_COPY(data_out, payload);
    } else {
        ZVAL_NULL(data_out);
    }
    zval_ptr_dtor(&decoded);
    return true;
}

static void kislay_queue_event_locked(php_kislay_socket_server_t *server,
                                      const std::string &sid,
                                      const std::string &event,
                                      zval *payload,
                                      std::vector<kislay_pending_call> &pending,
                                      const kislay_socket_session *session_ptr = nullptr) {
    // ZVAL_COPY below touches Zend refcounting; see kislay_socket_php_call_lock.
    std::lock_guard<std::recursive_mutex> php_guard(kislay_socket_php_call_lock);
    auto hit = server->handlers.find(event);
    auto ack_hit = server->ack_handlers.find(event);
    if (hit == server->handlers.end() && ack_hit == server->ack_handlers.end()) {
        return;
    }

    bool use_ack = (ack_hit != server->ack_handlers.end());
    kislay_pending_call call;
    call.event = event;
    call.sid = sid;
    call.needs_ack = use_ack;
    call.check_auth = false;
    if (use_ack) {
        ZVAL_COPY(&call.handler, &ack_hit->second);
    } else {
        ZVAL_COPY(&call.handler, &hit->second);
    }
    if (payload != nullptr) {
        ZVAL_COPY(&call.payload, payload);
        call.has_payload = true;
    } else {
        ZVAL_UNDEF(&call.payload);
        call.has_payload = false;
    }
    if (event == "connection" && server->has_auth_handler && session_ptr != nullptr) {
        call.check_auth = true;
        call.handshake_path = session_ptr->handshake_path;
        call.handshake_query_string = session_ptr->handshake_query_string;
        call.handshake_headers = session_ptr->handshake_headers;
    }
    pending.push_back(std::move(call));
}

static void kislay_run_pending_calls(php_kislay_socket_server_t *server,
                                     std::vector<kislay_pending_call> &pending) {
    // Covers the whole loop body, not just the kislay_call_php() calls inside
    // it - object_init_ex/ZVAL_COPY/zval_ptr_dtor throughout also touch Zend.
    // See kislay_socket_php_call_lock.
    std::lock_guard<std::recursive_mutex> php_guard(kislay_socket_php_call_lock);
    for (auto &call : pending) {
        // ── Auth gate ────────────────────────────────────────────────────────
        if (call.check_auth && server->has_auth_handler) {
            zval handshake, query_arr, headers_arr;
            array_init(&handshake);
            array_init(&query_arr);
            array_init(&headers_arr);
            std::unordered_map<std::string, std::string> qmap;
            kislay_parse_query(call.handshake_query_string.c_str(), qmap);
            for (const auto &kv : qmap) {
                add_assoc_string(&query_arr, kv.first.c_str(), kv.second.c_str());
            }
            for (const auto &kv : call.handshake_headers) {
                add_assoc_string(&headers_arr, kv.first.c_str(), kv.second.c_str());
            }
            add_assoc_zval(&handshake, "path", nullptr); // placeholder
            zval path_val; ZVAL_STRING(&path_val, call.handshake_path.c_str());
            zend_hash_str_update(Z_ARRVAL(handshake), "path", sizeof("path")-1, &path_val);
            add_assoc_zval(&handshake, "query", &query_arr);
            add_assoc_zval(&handshake, "headers", &headers_arr);
            zval sid_val; ZVAL_STRING(&sid_val, call.sid.c_str());
            zval auth_args[2]; ZVAL_COPY(&auth_args[0], &sid_val); ZVAL_COPY(&auth_args[1], &handshake);
            zval auth_ret; ZVAL_UNDEF(&auth_ret);
            kislay_call_php(&server->auth_handler, 2, auth_args, &auth_ret);
            zval_ptr_dtor(&sid_val);
            zval_ptr_dtor(&auth_args[0]);
            zval_ptr_dtor(&auth_args[1]);
            zval_ptr_dtor(&handshake);
            bool ok = !Z_ISUNDEF(auth_ret) && zend_is_true(&auth_ret);
            if (!Z_ISUNDEF(auth_ret)) { zval_ptr_dtor(&auth_ret); }
            if (!ok) {
                std::lock_guard<std::mutex> guard(server->lock);
                kislay_send_socketio_packet(server, call.sid, "1");
                kislay_remove_client(server, call.sid);
                zval_ptr_dtor(&call.handler);
                if (call.has_payload) { zval_ptr_dtor(&call.payload); }
                continue;
            }
        }

        zval socket_obj;
        object_init_ex(&socket_obj, kislay_socket_client_ce);
        php_kislay_socket_client_t *socket = php_kislay_socket_client_from_obj(Z_OBJ(socket_obj));
        socket->sid = call.sid;
        socket->server = server;

        bool one_arg = (call.event == "connection" || call.event == "disconnect");

        if (call.needs_ack && !one_arg) {
            // ── ACK path: pass 3rd arg (KislayAck object) ───────────────────
            zval ack_obj;
            object_init_ex(&ack_obj, kislay_ack_ce);
            php_kislay_ack_t *ack = php_kislay_ack_from_obj(Z_OBJ(ack_obj));
            ack->sid = call.sid;
            ack->server = server;
            zval args[3];
            ZVAL_COPY(&args[0], &socket_obj);
            if (call.has_payload) { ZVAL_COPY(&args[1], &call.payload); } else { ZVAL_NULL(&args[1]); }
            ZVAL_COPY(&args[2], &ack_obj);
            zval retval; ZVAL_UNDEF(&retval);
            kislay_call_php(&call.handler, 3, args, &retval);
            zval_ptr_dtor(&args[0]);
            zval_ptr_dtor(&args[1]);
            zval_ptr_dtor(&args[2]);
            zval_ptr_dtor(&ack_obj);
            if (!Z_ISUNDEF(retval)) { zval_ptr_dtor(&retval); }
        } else {
            zval args[2];
            ZVAL_COPY(&args[0], &socket_obj);
            if (!one_arg) {
                if (call.has_payload) { ZVAL_COPY(&args[1], &call.payload); } else { ZVAL_NULL(&args[1]); }
            }
            zval retval; ZVAL_UNDEF(&retval);
            kislay_call_php(&call.handler, one_arg ? 1 : 2, args, &retval);
            zval_ptr_dtor(&args[0]);
            if (!one_arg) { zval_ptr_dtor(&args[1]); }
            if (!Z_ISUNDEF(retval)) { zval_ptr_dtor(&retval); }
        }

        zval_ptr_dtor(&socket_obj);
        zval_ptr_dtor(&call.handler);
        if (call.has_payload) { zval_ptr_dtor(&call.payload); }
    }
    pending.clear();
}

static void kislay_remove_client(php_kislay_socket_server_t *server, const std::string &sid) {
    auto client_it = server->clients.find(sid);
    if (client_it == server->clients.end()) {
        return;
    }
    for (const auto &room : client_it->second.rooms) {
        auto rit = server->rooms.find(room);
        if (rit != server->rooms.end()) {
            rit->second.erase(sid);
            if (rit->second.empty()) {
                server->rooms.erase(rit);
            }
        }
    }
    server->clients.erase(client_it);
}

static void kislay_handle_socketio_packet(php_kislay_socket_server_t *server,
                                          const std::string &sid,
                                          kislay_socket_session &session,
                                          const std::string &packet,
                                          std::vector<kislay_pending_call> &pending) {
    if (packet.empty()) {
        return;
    }

    char type = packet[0];
    if (type == '0') {
        if (server->clients.find(sid) == server->clients.end()) {
            kislay_socket_client_state state;
            state.conn = session.ws_conn;
            state.sid = sid;
            server->clients.emplace(sid, state);
        }
        kislay_send_socketio_packet(server, sid, "0");
        {
            auto sit = server->sessions.find(sid);
            const kislay_socket_session *sptr = (sit != server->sessions.end()) ? &sit->second : nullptr;
            kislay_queue_event_locked(server, sid, "connection", nullptr, pending, sptr);
        }
        return;
    }

    if (type == '1') {
        kislay_queue_event_locked(server, sid, "disconnect", nullptr, pending);
        kislay_remove_client(server, sid);
        return;
    }

    if (type != '2' && type != '5') {
        return;
    }

    std::string event;
    zval payload;
    ZVAL_UNDEF(&payload);
    int attachments = 0;
    bool is_binary = false;

    if (!kislay_parse_socketio_event_packet(packet.data(), packet.size(), event, &payload, &attachments, &is_binary)) {
        if (!Z_ISUNDEF(payload)) {
            zval_ptr_dtor(&payload);
        }
        return;
    }

    if (is_binary && attachments > 0) {
        kislay_clear_pending(session.pending);
        session.pending.active = true;
        session.pending.expected = attachments;
        session.pending.received = 0;
        session.pending.event = event;
        if (Z_ISUNDEF(payload)) {
            ZVAL_NULL(&session.pending.payload);
        } else {
            ZVAL_COPY(&session.pending.payload, &payload);
        }
        if (!Z_ISUNDEF(payload)) {
            zval_ptr_dtor(&payload);
        }
        return;
    }

    if (!Z_ISUNDEF(payload)) {
        kislay_queue_event_locked(server, sid, event, &payload, pending);
        zval_ptr_dtor(&payload);
    } else {
        kislay_queue_event_locked(server, sid, event, nullptr, pending);
    }
}

static void kislay_handle_socketio_binary(php_kislay_socket_server_t *server,
                                          const std::string &sid,
                                          kislay_socket_session &session,
                                          const char *data,
                                          size_t data_len,
                                          std::vector<kislay_pending_call> &pending) {
    if (!session.pending.active || session.pending.expected <= 0) {
        return;
    }

    session.pending.binaries.emplace_back(data, data_len);
    session.pending.received += 1;
    if (session.pending.received < session.pending.expected) {
        return;
    }

    kislay_replace_placeholders(&session.pending.payload, session.pending.binaries);
    kislay_queue_event_locked(server, sid, session.pending.event, &session.pending.payload, pending);
    kislay_clear_pending(session.pending);
}

static bool kislay_handle_engineio_packet(php_kislay_socket_server_t *server,
                                          const std::string &sid,
                                          kislay_socket_session &session,
                                          const char *data,
                                          size_t data_len,
                                          std::vector<kislay_pending_call> &pending) {
    if (data_len == 0) {
        return false;
    }

    char type = data[0];
    if (type == '2') {
        session.last_pong = std::chrono::steady_clock::now();
        std::string pong = "3";
        if (data_len > 1) {
            pong.append(data + 1, data_len - 1);
        }
        kislay_engineio_send_packet(server, session, pong);
        return false;
    }

    if (type == '3') {
        session.last_pong = std::chrono::steady_clock::now();
        return false;
    }

    if (type == '5') {
        session.ws_upgraded = true;
        if (session.ws_conn != nullptr && !session.queue.empty()) {
            for (const auto &packet : session.queue) {
                mg_websocket_write(session.ws_conn, MG_WEBSOCKET_OPCODE_TEXT, packet.data(), packet.size());
            }
            session.queue.clear();
        }
        return false;
    }

    if (type == '1') {
        kislay_queue_event_locked(server, sid, "disconnect", nullptr, pending);
        kislay_remove_client(server, sid);
        return true;
    }

    if (type == '6') {
        return false;
    }

    if (type == '4') {
        if (data_len > 1 && data[1] == 'b') {
            zend_string *decoded = php_base64_decode_ex(reinterpret_cast<const unsigned char *>(data + 2), data_len - 2, 0);
            if (decoded != nullptr) {
                kislay_handle_socketio_binary(server, sid, session, ZSTR_VAL(decoded), ZSTR_LEN(decoded), pending);
                zend_string_release(decoded);
            }
            return false;
        }
        kislay_handle_socketio_packet(server, sid, session, std::string(data + 1, data_len - 1), pending);
        return false;
    }

    if (type == 'b') {
        zend_string *decoded = php_base64_decode_ex(reinterpret_cast<const unsigned char *>(data + 1), data_len - 1, 0);
        if (decoded != nullptr) {
            kislay_handle_socketio_binary(server, sid, session, ZSTR_VAL(decoded), ZSTR_LEN(decoded), pending);
            zend_string_release(decoded);
        }
    }

    return false;
}

static bool kislay_validate_auth(php_kislay_socket_server_t *server,
                                 const struct mg_connection *conn,
                                 const std::unordered_map<std::string, std::string> &query) {
    if (!server->auth_enabled) {
        return true;
    }
    if (server->auth_token.empty()) {
        return false;
    }

    std::string token;
    for (const auto &key : server->auth_query_keys) {
        auto it = query.find(key);
        if (it != query.end() && !it->second.empty()) {
            token = it->second;
            break;
        }
    }
    if (token.empty()) {
        for (const auto &key : server->auth_header_keys) {
            if (key.empty()) {
                continue;
            }
            std::string header_name = key;
            header_name[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(header_name[0])));
            for (size_t i = 1; i < header_name.size(); ++i) {
                if (header_name[i - 1] == '-') {
                    header_name[i] = static_cast<char>(std::toupper(static_cast<unsigned char>(header_name[i])));
                }
            }
            const char *header = mg_get_header(conn, header_name.c_str());
            if (header != nullptr) {
                std::string auth(header);
                const std::string bearer = "Bearer ";
                if (auth.rfind(bearer, 0) == 0) {
                    token = auth.substr(bearer.size());
                } else {
                    token = auth;
                }
                break;
            }
        }
    }

    return token == server->auth_token;
}

static void kislay_send_http_response(struct mg_connection *conn, int status, const std::string &body, bool cors_enabled) {
    const char *status_text = "OK";
    if (status == 400) {
        status_text = "Bad Request";
    } else if (status == 401) {
        status_text = "Unauthorized";
    } else if (status == 404) {
        status_text = "Not Found";
    } else if (status == 405) {
        status_text = "Method Not Allowed";
    }

    if (cors_enabled) {
        mg_printf(conn,
                  "HTTP/1.1 %d %s\r\n"
                  "Access-Control-Allow-Origin: *\r\n"
                  "Access-Control-Allow-Private-Network: true\r\n"
                  "Access-Control-Allow-Headers: *\r\n"
                  "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
                  "Content-Type: text/plain; charset=utf-8\r\n"
                  "Content-Length: %zu\r\n"
                  "Connection: close\r\n\r\n"
                  "%s",
                  status,
                  status_text,
                  body.size(),
                  body.c_str());
    } else {
        mg_printf(conn,
                  "HTTP/1.1 %d %s\r\n"
                  "Content-Type: text/plain; charset=utf-8\r\n"
                  "Content-Length: %zu\r\n"
                  "Connection: close\r\n\r\n"
                  "%s",
                  status,
                  status_text,
                  body.size(),
                  body.c_str());
    }
}

static int kislay_http_handler(struct mg_connection *conn, void *cbdata) {
    auto *server = static_cast<php_kislay_socket_server_t *>(cbdata);
    const struct mg_request_info *ri = mg_get_request_info(conn);
    if (ri == nullptr) {
        return 0;
    }

    std::unordered_map<std::string, std::string> query;
    kislay_parse_query(ri->query_string, query);

    if (!kislay_validate_auth(server, conn, query)) {
        kislay_send_http_response(conn, 401, "Unauthorized", server->cors_enabled);
        return 1;
    }

    auto transport_it = query.find("transport");
    if (transport_it == query.end()) {
        kislay_send_http_response(conn, 400, "Missing transport", server->cors_enabled);
        return 1;
    }
    const std::string &transport = transport_it->second;
    if (server->transports.find(kislay_to_lower(transport)) == server->transports.end()) {
        kislay_send_http_response(conn, 400, "Transport not allowed", server->cors_enabled);
        return 1;
    }
    if (transport != "polling") {
        return 0;
    }

    std::string method = ri->request_method ? ri->request_method : "";
    if (method == "OPTIONS") {
        kislay_send_http_response(conn, 200, "", server->cors_enabled);
        return 1;
    }

    if (method == "GET") {
        // Deliberately NOT holding kislay_socket_php_call_lock here: this
        // branch never touches Zend (no packet parsing/dispatch, just
        // session bookkeeping and reading session.queue), and it blocks for
        // up to ping_interval_ms inside cv.wait_for() below. cv.wait_for()
        // only releases the std::mutex passed to it (server->lock) while
        // waiting - it would NOT release a lock_guard-held
        // kislay_socket_php_call_lock, which would starve the Redis
        // subscriber thread out of ever acquiring it to deliver a message
        // for the whole duration of every long poll.
        std::unique_lock<std::mutex> lock(server->lock);
        auto sid_it = query.find("sid");
        if (sid_it == query.end()) {
            std::string sid = kislay_generate_sid(server);
            kislay_socket_session session;
            session.sid = sid;
            session.ws_conn = nullptr;
            session.ws_upgraded = false;
            session.queue.clear();
            session.pending.active = false;
            session.pending.expected = 0;
            session.pending.received = 0;
            ZVAL_UNDEF(&session.pending.payload);
            session.last_ping = std::chrono::steady_clock::now();
            session.last_pong = session.last_ping;
            kislay_capture_handshake(conn, session);
            server->sessions.emplace(sid, session);

            std::string open_packet = kislay_build_open_packet(sid, server->ping_interval_ms, server->ping_timeout_ms, server->max_payload,
                                                               server->allow_upgrade && server->transports.find("websocket") != server->transports.end());
            lock.unlock();
            kislay_send_http_response(conn, 200, open_packet, server->cors_enabled);
            return 1;
        }

        auto session_it = server->sessions.find(sid_it->second);
        if (session_it == server->sessions.end()) {
            lock.unlock();
            kislay_send_http_response(conn, 400, "Unknown sid", server->cors_enabled);
            return 1;
        }

        if (session_it->second.queue.empty()) {
            int wait_ms = server->ping_interval_ms > 0 ? server->ping_interval_ms : 25000;
            server->cv.wait_for(lock, std::chrono::milliseconds(wait_ms), [&]() {
                auto it = server->sessions.find(sid_it->second);
                return it != server->sessions.end() && !it->second.queue.empty();
            });
            session_it = server->sessions.find(sid_it->second);
            if (session_it == server->sessions.end()) {
                lock.unlock();
                kislay_send_http_response(conn, 400, "Unknown sid", server->cors_enabled);
                return 1;
            }
        }

        std::string payload;
        if (!session_it->second.queue.empty()) {
            payload = kislay_engineio_encode_payload(session_it->second.queue);
            session_it->second.queue.clear();
        } else {
            payload = "6";
        }
        lock.unlock();
        kislay_send_http_response(conn, 200, payload, server->cors_enabled);
        return 1;
    }

    if (method == "POST") {
        auto sid_it = query.find("sid");
        if (sid_it == query.end()) {
            kislay_send_http_response(conn, 400, "Missing sid", server->cors_enabled);
            return 1;
        }

        if (server->max_payload > 0 && ri->content_length > static_cast<long long>(server->max_payload)) {
            kislay_send_http_response(conn, 413, "Payload Too Large", server->cors_enabled);
            return 1;
        }

        std::vector<char> body;
        if (ri->content_length > 0) {
            body.resize(static_cast<size_t>(ri->content_length));
            size_t read_total = 0;
            while (read_total < body.size()) {
                int read_now = mg_read(conn, body.data() + read_total, body.size() - read_total);
                if (read_now <= 0) {
                    break;
                }
                read_total += static_cast<size_t>(read_now);
            }
            body.resize(read_total);
        }

        std::vector<kislay_pending_call> pending;
        // Acquired before server->lock, always in this order (also the order
        // used by the Redis subscriber thread) to avoid a lock-order
        // inversion between this mutex and server->lock. See
        // kislay_socket_php_call_lock.
        std::lock_guard<std::recursive_mutex> php_guard(kislay_socket_php_call_lock);
        std::unique_lock<std::mutex> lock(server->lock);
        auto session_it = server->sessions.find(sid_it->second);
        if (session_it == server->sessions.end()) {
            lock.unlock();
            kislay_send_http_response(conn, 400, "Unknown sid", server->cors_enabled);
            return 1;
        }

        std::vector<std::string> packets = kislay_engineio_parse_payload(body.data(), body.size());
        if (packets.empty() && !body.empty()) {
            packets.emplace_back(body.data(), body.size());
        }
        for (const auto &packet : packets) {
            bool closed = kislay_handle_engineio_packet(server, sid_it->second, session_it->second, packet.data(), packet.size(), pending);
            if (closed) {
                kislay_clear_pending(session_it->second.pending);
                server->sessions.erase(session_it);
                break;
            }
        }
        lock.unlock();
        kislay_run_pending_calls(server, pending);
        kislay_send_http_response(conn, 200, "ok", server->cors_enabled);
        return 1;
    }

    kislay_send_http_response(conn, 405, "Method not allowed", server->cors_enabled);
    return 1;
}

static int kislay_ws_connect_handler(const struct mg_connection *conn, void *cbdata) {
    auto *server = static_cast<php_kislay_socket_server_t *>(cbdata);
    const struct mg_request_info *ri = mg_get_request_info(conn);
    if (ri == nullptr) {
        return 1;
    }

    if (!server->allow_upgrade) {
        return 1;
    }

    std::unordered_map<std::string, std::string> query;
    kislay_parse_query(ri->query_string, query);

    if (!kislay_validate_auth(server, conn, query)) {
        return 1;
    }

    auto it = query.find("transport");
    if (it == query.end() || kislay_to_lower(it->second) != "websocket") {
        return 1;
    }
    if (server->transports.find("websocket") == server->transports.end()) {
        return 1;
    }

    return 0;
}

static void kislay_ws_ready_handler(struct mg_connection *conn, void *cbdata) {
    auto *server = static_cast<php_kislay_socket_server_t *>(cbdata);
    std::lock_guard<std::mutex> guard(server->lock);

    const struct mg_request_info *ri = mg_get_request_info(conn);
    std::unordered_map<std::string, std::string> query;
    if (ri != nullptr) {
        kislay_parse_query(ri->query_string, query);
    }

    std::string sid;
    auto sid_it = query.find("sid");
    if (sid_it != query.end()) {
        sid = sid_it->second;
    }

    if (sid.empty()) {
        sid = kislay_generate_sid(server);
        kislay_socket_session session;
        session.sid = sid;
        session.ws_conn = conn;
        session.ws_upgraded = true;
        session.queue.clear();
        session.pending.active = false;
        session.pending.expected = 0;
        session.pending.received = 0;
        ZVAL_UNDEF(&session.pending.payload);
        session.last_ping = std::chrono::steady_clock::now();
        session.last_pong = session.last_ping;
        kislay_capture_handshake(conn, session);
        server->sessions.emplace(sid, session);

        std::string open_packet = kislay_build_open_packet(sid, server->ping_interval_ms, server->ping_timeout_ms, server->max_payload,
                                   server->allow_upgrade && server->transports.find("websocket") != server->transports.end());
        mg_websocket_write(conn, MG_WEBSOCKET_OPCODE_TEXT, open_packet.data(), open_packet.size());
    } else {
        auto session_it = server->sessions.find(sid);
        if (session_it == server->sessions.end()) {
            kislay_socket_session session;
            session.sid = sid;
            session.ws_conn = conn;
            session.ws_upgraded = false;
            session.queue.clear();
            session.pending.active = false;
            session.pending.expected = 0;
            session.pending.received = 0;
            ZVAL_UNDEF(&session.pending.payload);
            session.last_ping = std::chrono::steady_clock::now();
            session.last_pong = session.last_ping;
            server->sessions.emplace(sid, session);
        } else {
            session_it->second.ws_conn = conn;
        }
    }

    server->conn_to_sid.emplace(conn, sid);

    auto client_it = server->clients.find(sid);
    if (client_it != server->clients.end()) {
        client_it->second.conn = conn;
    }
}

static int kislay_ws_data_handler(struct mg_connection *conn, int bits, char *data, size_t data_len, void *cbdata) {
#ifdef ZTS
    ZEND_TSRMLS_CACHE_UPDATE();
#endif

    auto *server = static_cast<php_kislay_socket_server_t *>(cbdata);
    std::vector<kislay_pending_call> pending;
    // See kislay_socket_php_call_lock for why this is acquired before
    // server->lock, consistently, everywhere.
    std::lock_guard<std::recursive_mutex> php_guard(kislay_socket_php_call_lock);
    std::unique_lock<std::mutex> lock(server->lock);
    auto sid_it = server->conn_to_sid.find(conn);
    if (sid_it == server->conn_to_sid.end()) {
        return 1;
    }
    auto session_it = server->sessions.find(sid_it->second);
    if (session_it == server->sessions.end()) {
        return 1;
    }

    if (server->max_payload > 0 && data_len > server->max_payload) {
        return 1;
    }

    if ((bits & MG_WEBSOCKET_OPCODE_TEXT) == 0) {
        if ((bits & MG_WEBSOCKET_OPCODE_BINARY) != 0) {
            kislay_handle_socketio_binary(server, sid_it->second, session_it->second, data, data_len, pending);
        }
        lock.unlock();
        kislay_run_pending_calls(server, pending);
        return 1;
    }

    std::vector<std::string> packets = kislay_engineio_parse_payload(data, data_len);
    if (packets.empty()) {
        bool closed = kislay_handle_engineio_packet(server, sid_it->second, session_it->second, data, data_len, pending);
        if (closed) {
            kislay_clear_pending(session_it->second.pending);
            server->sessions.erase(session_it);
        }
    } else {
        for (const auto &packet : packets) {
            bool closed = kislay_handle_engineio_packet(server, sid_it->second, session_it->second, packet.data(), packet.size(), pending);
            if (closed) {
                kislay_clear_pending(session_it->second.pending);
                server->sessions.erase(session_it);
                break;
            }
        }
    }
    lock.unlock();
    kislay_run_pending_calls(server, pending);
    return 1;
}

static void kislay_ws_close_handler(const struct mg_connection *conn, void *cbdata) {
    auto *server = static_cast<php_kislay_socket_server_t *>(cbdata);
    std::vector<kislay_pending_call> pending;
    // See kislay_socket_php_call_lock for why this is acquired before
    // server->lock, consistently, everywhere.
    std::lock_guard<std::recursive_mutex> php_guard(kislay_socket_php_call_lock);
    std::unique_lock<std::mutex> lock(server->lock);

    auto sid_it = server->conn_to_sid.find(const_cast<struct mg_connection *>(conn));
    if (sid_it == server->conn_to_sid.end()) {
        return;
    }

    std::string sid = sid_it->second;
    server->conn_to_sid.erase(sid_it);

    auto session_it = server->sessions.find(sid);
    if (session_it != server->sessions.end()) {
        session_it->second.ws_conn = nullptr;
        session_it->second.ws_upgraded = false;
        kislay_queue_event_locked(server, sid, "disconnect", nullptr, pending);
        kislay_remove_client(server, sid);
        kislay_clear_pending(session_it->second.pending);
        server->sessions.erase(session_it);
    }
    lock.unlock();
    kislay_run_pending_calls(server, pending);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_void, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_on, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, event, IS_STRING, 0)
    ZEND_ARG_CALLABLE_INFO(0, handler, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_emit, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, event, IS_STRING, 0)
    ZEND_ARG_INFO(0, data)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_emit_room, 0, 0, 3)
    ZEND_ARG_TYPE_INFO(0, room, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, event, IS_STRING, 0)
    ZEND_ARG_INFO(0, data)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_listen, 0, 0, 3)
    ZEND_ARG_TYPE_INFO(0, host, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, port, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, path, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_join, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, room, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_id, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_kislay_socket_client_count, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_kislay_socket_room_count, 0, 1, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, room, IS_STRING, 0)
ZEND_END_ARG_INFO()


ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_on_auth, 0, 0, 1)
    ZEND_ARG_CALLABLE_INFO(0, handler, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_on_with_ack, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, event, IS_STRING, 0)
    ZEND_ARG_CALLABLE_INFO(0, handler, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_kislay_socket_get_clients, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_set_max_payload, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, bytes, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_socket_namespace, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, ns, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislay_ack_invoke, 0, 0, 0)
    ZEND_ARG_INFO(0, data)
ZEND_END_ARG_INFO()

PHP_METHOD(KislaySocketServer, __construct) {
    ZEND_PARSE_PARAMETERS_NONE();
}

PHP_METHOD(KislaySocketServer, on) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *handler = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(handler)
    ZEND_PARSE_PARAMETERS_END();

    if (!kislay_is_callable(handler)) {
        zend_throw_exception(zend_ce_exception, "Handler must be callable", 0);
        RETURN_FALSE;
    }

    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
    std::lock_guard<std::mutex> guard(server->lock);
    std::string key(event, event_len);
    auto it = server->handlers.find(key);
    if (it != server->handlers.end()) {
        zval_ptr_dtor(&it->second);
        server->handlers.erase(it);
    }
    zval copy;
    ZVAL_COPY(&copy, handler);
    server->handlers[key] = copy;
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, emit) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_broadcast(server, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, publish) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_broadcast(server, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, send) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_broadcast(server, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, emitTo) {
    char *room = nullptr;
    size_t room_len = 0;
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(room, room_len)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string topic(room, room_len);
        topic.append(":");
        topic.append(std::string(event, event_len));
        std::string error;
        if (kislay_rpc_publish(topic, data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_emit_room(server, std::string(room, room_len), std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, listen) {
    char *host = nullptr;
    size_t host_len = 0;
    zend_long port = 0;
    char *path = nullptr;
    size_t path_len = 0;
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(host, host_len)
        Z_PARAM_LONG(port)
        Z_PARAM_STRING(path, path_len)
    ZEND_PARSE_PARAMETERS_END();

    if (port <= 0 || port > 65535) {
        zend_throw_exception(zend_ce_exception, "Invalid port", 0);
        RETURN_FALSE;
    }

    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
    if (server->ctx != nullptr) {
        zend_throw_exception(zend_ce_exception, "Server already running", 0);
        RETURN_FALSE;
    }

    std::string listen_addr = std::string(host, host_len) + ":" + std::to_string(port);
    server->path.assign(path, path_len);

    std::vector<const char *> options;
    std::string opt_port = listen_addr;
    options.push_back("listening_ports");
    options.push_back(opt_port.c_str());
    options.push_back("num_threads");
    options.push_back("1");
    // civetweb's mg_websocket_write_exec() sends the frame header and payload
    // as two separate mg_write() calls. Without tcp_nodelay, Nagle holds the
    // small payload write until the client ACKs the header write, and that
    // ACK is itself delayed (~40ms) with nothing to piggyback on - the same
    // Nagle/delayed-ACK interaction found and fixed in the Gateway extension
    // (see gateway's PHP_METHOD(KislayPHPGateway, listen)). For a realtime
    // socket transport this would tax every single outgoing message.
    options.push_back("tcp_nodelay");
    options.push_back("1");
    options.push_back(nullptr);

    server->ctx = mg_start(nullptr, server, options.data());
    if (server->ctx == nullptr) {
        zend_throw_exception(zend_ce_exception, "Failed to start server", 0);
        RETURN_FALSE;
    }

    mg_set_request_handler(server->ctx, server->path.c_str(), kislay_http_handler, server);
    mg_set_websocket_handler(server->ctx,
                             server->path.c_str(),
                             kislay_ws_connect_handler,
                             kislay_ws_ready_handler,
                             kislay_ws_data_handler,
                             kislay_ws_close_handler,
                             server);

    server->running = true;
    while (server->running) {
        std::vector<kislay_pending_call> pending;
        {
            std::lock_guard<std::mutex> guard(server->lock);
            auto now = std::chrono::steady_clock::now();
            std::vector<std::string> expired;
            for (auto &entry : server->sessions) {
                auto &session = entry.second;
                auto ping_age = std::chrono::duration_cast<std::chrono::milliseconds>(now - session.last_ping).count();
                auto pong_age = std::chrono::duration_cast<std::chrono::milliseconds>(now - session.last_pong).count();
                int ping_interval = server->ping_interval_ms > 0 ? server->ping_interval_ms : 25000;
                int ping_timeout = server->ping_timeout_ms > 0 ? server->ping_timeout_ms : 20000;
                if (ping_age >= ping_interval) {
                    kislay_engineio_send_packet(server, session, "2");
                    session.last_ping = now;
                }
                if (pong_age > (ping_interval + ping_timeout)) {
                    expired.push_back(entry.first);
                }
            }

            for (const auto &sid : expired) {
                auto sit = server->sessions.find(sid);
                if (sit == server->sessions.end()) {
                    continue;
                }
                kislay_engineio_send_packet(server, sit->second, "1");
                if (sit->second.ws_conn != nullptr) {
                    server->conn_to_sid.erase(sit->second.ws_conn);
                }
                kislay_queue_event_locked(server, sid, "disconnect", nullptr, pending);
                kislay_remove_client(server, sid);
                kislay_clear_pending(sit->second.pending);
                server->sessions.erase(sit);
            }
        }
        kislay_run_pending_calls(server, pending);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, id) {
    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
    RETURN_STRING(client->sid.c_str());
}

PHP_METHOD(KislaySocketClient, join) {
    char *room = nullptr;
    size_t room_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(room, room_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::string room_name(room, room_len);
    std::lock_guard<std::mutex> guard(client->server->lock);
    auto cit = client->server->clients.find(client->sid);
    if (cit == client->server->clients.end()) {
        RETURN_FALSE;
    }
    cit->second.rooms.insert(room_name);
    client->server->rooms[room_name].insert(client->sid);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, leave) {
    char *room = nullptr;
    size_t room_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(room, room_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::string room_name(room, room_len);
    std::lock_guard<std::mutex> guard(client->server->lock);
    auto cit = client->server->clients.find(client->sid);
    if (cit == client->server->clients.end()) {
        RETURN_FALSE;
    }
    cit->second.rooms.erase(room_name);
    auto rit = client->server->rooms.find(room_name);
    if (rit != client->server->rooms.end()) {
        rit->second.erase(client->sid);
        if (rit->second.empty()) {
            client->server->rooms.erase(rit);
        }
    }
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, emit) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::lock_guard<std::mutex> guard(client->server->lock);
    auto cit = client->server->clients.find(client->sid);
    if (cit == client->server->clients.end()) {
        RETURN_FALSE;
    }
    kislay_send_socketio_event(client->server, client->sid, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, emitTo) {
    char *room = nullptr;
    size_t room_len = 0;
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(room, room_len)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string topic(room, room_len);
        topic.append(":");
        topic.append(std::string(event, event_len));
        std::string error;
        if (kislay_rpc_publish(topic, data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::lock_guard<std::mutex> guard(client->server->lock);
    kislay_emit_room(client->server, std::string(room, room_len), std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, publish) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::lock_guard<std::mutex> guard(client->server->lock);
    auto cit = client->server->clients.find(client->sid);
    if (cit == client->server->clients.end()) {
        RETURN_FALSE;
    }
    kislay_send_socketio_event(client->server, client->sid, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, send) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::lock_guard<std::mutex> guard(client->server->lock);
    auto cit = client->server->clients.find(client->sid);
    if (cit == client->server->clients.end()) {
        RETURN_FALSE;
    }
    kislay_send_socketio_event(client->server, client->sid, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketClient, reply) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();

    php_kislay_socket_client_t *client = php_kislay_socket_client_from_obj(Z_OBJ_P(getThis()));
#ifdef KISLAYPHP_RPC
    if (kislay_rpc_enabled()) {
        std::string error;
        if (kislay_rpc_publish(std::string(event, event_len), data, &error)) {
            RETURN_TRUE;
        }
    }
#endif
    if (client->server == nullptr) {
        RETURN_FALSE;
    }

    std::lock_guard<std::mutex> guard(client->server->lock);
    auto cit = client->server->clients.find(client->sid);
    if (cit == client->server->clients.end()) {
        RETURN_FALSE;
    }
    kislay_send_socketio_event(client->server, client->sid, std::string(event, event_len), data);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, clientCount) {
    php_kislay_socket_server_t *obj = php_kislay_socket_server_from_obj(Z_OBJ_P(ZEND_THIS));
    std::lock_guard<std::mutex> guard(obj->lock);
    zend_long count = (zend_long)obj->clients.size();
    RETURN_LONG(count);
}

PHP_METHOD(KislaySocketServer, roomCount) {
    char *room; size_t room_len;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(room, room_len)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_socket_server_t *obj = php_kislay_socket_server_from_obj(Z_OBJ_P(ZEND_THIS));
    std::lock_guard<std::mutex> guard(obj->lock);
    auto it = obj->rooms.find(std::string(room, room_len));
    zend_long count = (it != obj->rooms.end()) ? (zend_long)it->second.size() : 0;
    RETURN_LONG(count);
}


PHP_METHOD(KislaySocketServer, onAuth) {
    zval *handler = nullptr;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(handler)
    ZEND_PARSE_PARAMETERS_END();
    if (!kislay_is_callable(handler)) {
        zend_throw_exception(zend_ce_exception, "Handler must be callable", 0);
        RETURN_FALSE;
    }
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
    std::lock_guard<std::mutex> guard(server->lock);
    if (server->has_auth_handler) {
        zval_ptr_dtor(&server->auth_handler);
    }
    ZVAL_COPY(&server->auth_handler, handler);
    server->has_auth_handler = true;
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, onWithAck) {
    char *event = nullptr;
    size_t event_len = 0;
    zval *handler = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(handler)
    ZEND_PARSE_PARAMETERS_END();
    if (!kislay_is_callable(handler)) {
        zend_throw_exception(zend_ce_exception, "Handler must be callable", 0);
        RETURN_FALSE;
    }
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
    std::lock_guard<std::mutex> guard(server->lock);
    std::string key(event, event_len);
    auto it = server->ack_handlers.find(key);
    if (it != server->ack_handlers.end()) {
        zval_ptr_dtor(&it->second);
        server->ack_handlers.erase(it);
    }
    zval copy; ZVAL_COPY(&copy, handler);
    server->ack_handlers[key] = copy;
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, getClients) {
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
    std::lock_guard<std::mutex> guard(server->lock);
    array_init(return_value);
    for (const auto &entry : server->clients) {
        add_next_index_string(return_value, entry.first.c_str());
    }
}

PHP_METHOD(KislaySocketServer, setMaxPayload) {
    zend_long bytes = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(bytes)
    ZEND_PARSE_PARAMETERS_END();
    if (bytes < 0) { bytes = 0; }
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(getThis()));
    std::lock_guard<std::mutex> guard(server->lock);
    server->max_payload = static_cast<size_t>(bytes);
    RETURN_TRUE;
}

PHP_METHOD(KislaySocketServer, namespace) {
    char *ns = nullptr;
    size_t ns_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(ns, ns_len)
    ZEND_PARSE_PARAMETERS_END();
    object_init_ex(return_value, kislay_namespace_ce);
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(return_value));
    ZVAL_COPY(&nsobj->server_obj, getThis());
    nsobj->ns.assign(ns, ns_len);
}

// ── KislayAck methods ─────────────────────────────────────────────────────
PHP_METHOD(KislayAck, __invoke) {
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_ack_t *ack = php_kislay_ack_from_obj(Z_OBJ_P(getThis()));
    if (ack->server == nullptr) { RETURN_FALSE; }
    std::lock_guard<std::mutex> guard(ack->server->lock);
    if (data != nullptr) {
        kislay_send_socketio_event(ack->server, ack->sid, "__ack", data);
    } else {
        kislay_send_socketio_event(ack->server, ack->sid, "__ack", nullptr);
    }
    RETURN_TRUE;
}

// ── KislayNamespace methods ───────────────────────────────────────────────
PHP_METHOD(KislayNamespace, on) {
    char *event = nullptr; size_t event_len = 0;
    zval *handler = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(handler)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(getThis()));
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(&nsobj->server_obj));
    if (!kislay_is_callable(handler)) {
        zend_throw_exception(zend_ce_exception, "Handler must be callable", 0);
        RETURN_FALSE;
    }
    std::string key = nsobj->ns + ":" + std::string(event, event_len);
    std::lock_guard<std::mutex> guard(server->lock);
    auto it = server->handlers.find(key);
    if (it != server->handlers.end()) { zval_ptr_dtor(&it->second); server->handlers.erase(it); }
    zval copy; ZVAL_COPY(&copy, handler);
    server->handlers[key] = copy;
    RETURN_ZVAL(getThis(), 1, 0);
}

PHP_METHOD(KislayNamespace, onWithAck) {
    char *event = nullptr; size_t event_len = 0;
    zval *handler = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(handler)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(getThis()));
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(&nsobj->server_obj));
    if (!kislay_is_callable(handler)) {
        zend_throw_exception(zend_ce_exception, "Handler must be callable", 0);
        RETURN_FALSE;
    }
    std::string key = nsobj->ns + ":" + std::string(event, event_len);
    std::lock_guard<std::mutex> guard(server->lock);
    auto it = server->ack_handlers.find(key);
    if (it != server->ack_handlers.end()) { zval_ptr_dtor(&it->second); server->ack_handlers.erase(it); }
    zval copy; ZVAL_COPY(&copy, handler);
    server->ack_handlers[key] = copy;
    RETURN_ZVAL(getThis(), 1, 0);
}

PHP_METHOD(KislayNamespace, emit) {
    char *event = nullptr; size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(getThis()));
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(&nsobj->server_obj));
    std::string ev = nsobj->ns + ":" + std::string(event, event_len);
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_broadcast(server, ev, data);
    RETURN_TRUE;
}

PHP_METHOD(KislayNamespace, emitTo) {
    char *sid = nullptr; size_t sid_len = 0;
    char *event = nullptr; size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(sid, sid_len)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(getThis()));
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(&nsobj->server_obj));
    std::string ev = nsobj->ns + ":" + std::string(event, event_len);
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_send_socketio_event(server, std::string(sid, sid_len), ev, data);
    RETURN_TRUE;
}

PHP_METHOD(KislayNamespace, emitToRoom) {
    char *room = nullptr; size_t room_len = 0;
    char *event = nullptr; size_t event_len = 0;
    zval *data = nullptr;
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(room, room_len)
        Z_PARAM_STRING(event, event_len)
        Z_PARAM_ZVAL(data)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(getThis()));
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(&nsobj->server_obj));
    std::string ns_room = nsobj->ns + ":" + std::string(room, room_len);
    std::string ev = nsobj->ns + ":" + std::string(event, event_len);
    std::lock_guard<std::mutex> guard(server->lock);
    kislay_emit_room(server, ns_room, ev, data);
    RETURN_TRUE;
}

PHP_METHOD(KislayNamespace, join) {
    char *sid = nullptr; size_t sid_len = 0;
    char *room = nullptr; size_t room_len = 0;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(sid, sid_len)
        Z_PARAM_STRING(room, room_len)
    ZEND_PARSE_PARAMETERS_END();
    php_kislay_namespace_t *nsobj = php_kislay_namespace_from_obj(Z_OBJ_P(getThis()));
    php_kislay_socket_server_t *server = php_kislay_socket_server_from_obj(Z_OBJ_P(&nsobj->server_obj));
    std::string ns_room = nsobj->ns + ":" + std::string(room, room_len);
    std::string client_sid(sid, sid_len);
    std::lock_guard<std::mutex> guard(server->lock);
    auto cit = server->clients.find(client_sid);
    if (cit == server->clients.end()) { RETURN_FALSE; }
    cit->second.rooms.insert(ns_room);
    server->rooms[ns_room].insert(client_sid);
    RETURN_TRUE;
}

static const zend_function_entry kislay_socket_server_methods[] = {
    PHP_ME(KislaySocketServer, __construct, arginfo_kislay_socket_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, on, arginfo_kislay_socket_on, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, emit, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, publish, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, send, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, emitTo, arginfo_kislay_socket_emit_room, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, listen, arginfo_kislay_socket_listen, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, clientCount, arginfo_kislay_socket_client_count, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, roomCount, arginfo_kislay_socket_room_count, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, onAuth, arginfo_kislay_socket_on_auth, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, onWithAck, arginfo_kislay_socket_on_with_ack, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, getClients, arginfo_kislay_socket_get_clients, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, setMaxPayload, arginfo_kislay_socket_set_max_payload, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketServer, namespace, arginfo_kislay_socket_namespace, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislay_ack_methods[] = {
    PHP_ME(KislayAck, __invoke, arginfo_kislay_ack_invoke, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislay_namespace_methods[] = {
    PHP_ME(KislayNamespace, on, arginfo_kislay_socket_on, ZEND_ACC_PUBLIC)
    PHP_ME(KislayNamespace, onWithAck, arginfo_kislay_socket_on_with_ack, ZEND_ACC_PUBLIC)
    PHP_ME(KislayNamespace, emit, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislayNamespace, emitTo, arginfo_kislay_socket_emit_room, ZEND_ACC_PUBLIC)
    PHP_ME(KislayNamespace, emitToRoom, arginfo_kislay_socket_emit_room, ZEND_ACC_PUBLIC)
    PHP_ME(KislayNamespace, join, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislay_socket_client_methods[] = {
    PHP_ME(KislaySocketClient, id, arginfo_kislay_socket_id, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, join, arginfo_kislay_socket_join, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, leave, arginfo_kislay_socket_join, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, emit, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, publish, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, send, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, reply, arginfo_kislay_socket_emit, ZEND_ACC_PUBLIC)
    PHP_ME(KislaySocketClient, emitTo, arginfo_kislay_socket_emit_room, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

PHP_MINIT_FUNCTION(kislayphp_socket) {
    REGISTER_INI_ENTRIES();
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Socket", "Server", kislay_socket_server_methods);
    kislay_socket_server_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Socket\\Server", kislay_socket_server_ce);
    zend_register_class_alias("Kislay\\EventBus\\Server", kislay_socket_server_ce);
    zend_register_class_alias("KislayPHP\\EventBus\\Server", kislay_socket_server_ce);
    kislay_socket_server_ce->create_object = kislay_socket_server_create_object;
    std::memcpy(&kislay_socket_server_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislay_socket_server_handlers.offset = XtOffsetOf(php_kislay_socket_server_t, std);
    kislay_socket_server_handlers.free_obj = kislay_socket_server_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Socket", "Socket", kislay_socket_client_methods);
    kislay_socket_client_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Socket\\Socket", kislay_socket_client_ce);
    zend_register_class_alias("Kislay\\EventBus\\Socket", kislay_socket_client_ce);
    zend_register_class_alias("KislayPHP\\EventBus\\Socket", kislay_socket_client_ce);
    kislay_socket_client_ce->create_object = kislay_socket_client_create_object;
    std::memcpy(&kislay_socket_client_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislay_socket_client_handlers.offset = XtOffsetOf(php_kislay_socket_client_t, std);
    kislay_socket_client_handlers.free_obj = kislay_socket_client_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Socket", "Ack", kislay_ack_methods);
    kislay_ack_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Socket\\Ack", kislay_ack_ce);
    zend_register_class_alias("Kislay\\EventBus\\Ack", kislay_ack_ce);
    zend_register_class_alias("KislayPHP\\EventBus\\Ack", kislay_ack_ce);
    kislay_ack_ce->create_object = kislay_ack_create_object;
    std::memcpy(&kislay_ack_handlers_obj, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislay_ack_handlers_obj.offset = XtOffsetOf(php_kislay_ack_t, std);
    kislay_ack_handlers_obj.free_obj = kislay_ack_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Socket", "Namespace", kislay_namespace_methods);
    kislay_namespace_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Socket\\Namespace", kislay_namespace_ce);
    zend_register_class_alias("Kislay\\EventBus\\Namespace", kislay_namespace_ce);
    zend_register_class_alias("KislayPHP\\EventBus\\Namespace", kislay_namespace_ce);
    kislay_namespace_ce->create_object = kislay_namespace_create_object;
    std::memcpy(&kislay_namespace_handlers_obj, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislay_namespace_handlers_obj.offset = XtOffsetOf(php_kislay_namespace_t, std);
    kislay_namespace_handlers_obj.free_obj = kislay_namespace_free_obj;

    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(kislayphp_socket) {
    UNREGISTER_INI_ENTRIES();
    return SUCCESS;
}

PHP_MINFO_FUNCTION(kislayphp_socket) {
    php_info_print_table_start();
    php_info_print_table_header(2, "kislayphp_socket support", "enabled");
    php_info_print_table_row(2, "Version", PHP_KISLAYPHP_SOCKET_VERSION);
    php_info_print_table_end();
}

static PHP_GINIT_FUNCTION(kislayphp_socket) {
    kislayphp_socket_globals->ping_interval_ms = 25000;
    kislayphp_socket_globals->ping_timeout_ms = 20000;
    kislayphp_socket_globals->max_payload = 1000000;
    kislayphp_socket_globals->cors_enabled = 1;
    kislayphp_socket_globals->allow_upgrade = 1;
    kislayphp_socket_globals->transports = nullptr;
    kislayphp_socket_globals->auth_enabled = 0;
    kislayphp_socket_globals->auth_token = nullptr;
    kislayphp_socket_globals->auth_query_keys = nullptr;
    kislayphp_socket_globals->auth_header_keys = nullptr;
}

zend_module_entry kislayphp_socket_module_entry = {
    STANDARD_MODULE_HEADER,
    PHP_KISLAYPHP_SOCKET_EXTNAME,
    nullptr,
    PHP_MINIT(kislayphp_socket),
    PHP_MSHUTDOWN(kislayphp_socket),
    nullptr,
    nullptr,
    PHP_MINFO(kislayphp_socket),
    PHP_KISLAYPHP_SOCKET_VERSION,
    PHP_MODULE_GLOBALS(kislayphp_socket),
    PHP_GINIT(kislayphp_socket),
    nullptr,
    nullptr,
    STANDARD_MODULE_PROPERTIES_EX
};

#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE();
#endif

extern "C" {
ZEND_DLEXPORT zend_module_entry *get_module(void) {
    return &kislayphp_socket_module_entry;
}
}
