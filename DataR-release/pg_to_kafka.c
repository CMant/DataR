/* ===================================================================
 *  pg_to_kafka.c — PG → Kafka 迁移（基于 pg_engine）
 *
 *  TASK_INF（MyTaskCfg）定义、创建、释放完全在此文件内。
 * =================================================================== */

#include "include/pg_engine.h"
#include "include/common.h"
#include "include/network_service.h"
#include "include/cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <librdkafka/rdkafka.h>

/* ===================================================================
 *  本地任务配置 + Kafka 特有参数
 * =================================================================== */
typedef struct {
    char *task_name;
    char *task_speed;
    char *migrate_type;
    char *pump_sql;
    int   parallel_thread_per_task;
    int   send_buffer_size;
    char *source_host;
    char *source_port;
    char *source_dbname;
    char *source_password;
    char *source_username;

    /* Kafka 特有 */
    char *kafka_server;
    char *kafka_queue_buffering_max_messages;
    char *kafka_queue_buffering_max_kbytes;
    char *kafka_batch_num_messages;
    char *kafka_queue_enqueue_timeout_ms;
    char *kafka_message_timeout_ms;
    char *kafka_retries;
    char *kafka_retry_backoff_ms;
    char *kafka_socket_send_buffer_bytes;
    char *kafka_socket_receive_buffer_bytes;
    char *kafka_socket_connection_setup_timeout_ms;
    char *kafka_metadata_max_age_ms;
    char *kafka_compression_codec;
    char *kafka_compression_level;
    char *kafka_security_protocol;
    char *kafka_enable_ssl_certificate_verification;
    char *kafka_ssl_cipher_suites;
    char *kafka_ssl_key_location;
    char *kafka_ssl_key_password;
    char *kafka_ssl_certificate_location;
    char *kafka_ssl_ca_location;
    char *kafka_ssl_endpoint_identification_algorithm;
    char *kafka_sasl_mechanism;
    char *kafka_sasl_username;
    char *kafka_sasl_password;
    char *kafka_client_id;
    char *kafka_log_level;
    char *kafka_debug;
} MyTaskCfg;

static void my_task_free(MyTaskCfg *cfg)
{
    if (!cfg) return;
    g_free(cfg->task_name);
    g_free(cfg->task_speed);
    g_free(cfg->migrate_type);
    g_free(cfg->pump_sql);
    g_free(cfg->source_host);
    g_free(cfg->source_port);
    g_free(cfg->source_dbname);
    g_free(cfg->source_password);
    g_free(cfg->source_username);
    g_free(cfg->kafka_server);
    g_free(cfg->kafka_queue_buffering_max_messages);
    g_free(cfg->kafka_queue_buffering_max_kbytes);
    g_free(cfg->kafka_batch_num_messages);
    g_free(cfg->kafka_queue_enqueue_timeout_ms);
    g_free(cfg->kafka_message_timeout_ms);
    g_free(cfg->kafka_retries);
    g_free(cfg->kafka_retry_backoff_ms);
    g_free(cfg->kafka_socket_send_buffer_bytes);
    g_free(cfg->kafka_socket_receive_buffer_bytes);
    g_free(cfg->kafka_socket_connection_setup_timeout_ms);
    g_free(cfg->kafka_metadata_max_age_ms);
    g_free(cfg->kafka_compression_codec);
    g_free(cfg->kafka_compression_level);
    g_free(cfg->kafka_security_protocol);
    g_free(cfg->kafka_enable_ssl_certificate_verification);
    g_free(cfg->kafka_ssl_cipher_suites);
    g_free(cfg->kafka_ssl_key_location);
    g_free(cfg->kafka_ssl_key_password);
    g_free(cfg->kafka_ssl_certificate_location);
    g_free(cfg->kafka_ssl_ca_location);
    g_free(cfg->kafka_ssl_endpoint_identification_algorithm);
    g_free(cfg->kafka_sasl_mechanism);
    g_free(cfg->kafka_sasl_username);
    g_free(cfg->kafka_sasl_password);
    g_free(cfg->kafka_client_id);
    g_free(cfg->kafka_log_level);
    g_free(cfg->kafka_debug);
}

/* ===================================================================
 *  Kafka 全局句柄
 * =================================================================== */
static rd_kafka_t      *g_rk   = NULL;
static rd_kafka_topic_t *g_rkt = NULL;
static int g_kafka_initialized = 0;

static int message_count       = 0;
static int max_message_count   = 2147483647;
static int max_memory_kbytes   = 1048576;
static long long local_memory_usage = 0;

/* ===================================================================
 *  Kafka 初始化/关闭/发送
 * =================================================================== */
void kafka_close(void)
{
    if (g_rkt) { rd_kafka_topic_destroy(g_rkt); g_rkt = NULL; }
    if (g_rk)  { rd_kafka_flush(g_rk, 5000); rd_kafka_destroy(g_rk); g_rk = NULL; }
    message_count = 0; local_memory_usage = 0; g_kafka_initialized = 0;
}

static int kafka_init(MyTaskCfg *cfg, const char *task_name)
{
    char errstr[512] = {0};
    if (!cfg->kafka_server || !task_name) return -1;

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf) return -1;

    if (rd_kafka_conf_set(conf, "bootstrap.servers", cfg->kafka_server,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    { rd_kafka_conf_destroy(conf); return -1; }

    max_message_count = atoi(cfg->kafka_queue_buffering_max_messages ?: "100000");
    max_memory_kbytes = atoi(cfg->kafka_queue_buffering_max_kbytes ?: "1048576");

    rd_kafka_conf_set(conf, "message.max.bytes", "1000000000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", cfg->kafka_queue_buffering_max_messages ?: "100000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", cfg->kafka_queue_buffering_max_kbytes ?: "1048576", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "batch.num.messages", cfg->kafka_batch_num_messages ?: "10000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.enqueue.timeout.ms", cfg->kafka_queue_enqueue_timeout_ms ?: "5", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "message.timeout.ms", cfg->kafka_message_timeout_ms ?: "300000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "retries", cfg->kafka_retries ?: "2", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "retry.backoff.ms", cfg->kafka_retry_backoff_ms ?: "100", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "socket.send.buffer.bytes", cfg->kafka_socket_send_buffer_bytes ?: "65536", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "socket.receive.buffer.bytes", cfg->kafka_socket_receive_buffer_bytes ?: "65536", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "socket.connection.setup.timeout.ms", cfg->kafka_socket_connection_setup_timeout_ms ?: "10000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "metadata.max.age.ms", cfg->kafka_metadata_max_age_ms ?: "300000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "compression.codec", cfg->kafka_compression_codec ?: "none", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "compression.level", cfg->kafka_compression_level ?: "-1", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "real.time.queue", "0", errstr, sizeof(errstr));

    char *sp = cfg->kafka_security_protocol;
    if (sp && strlen(sp) > 0)
    {
        rd_kafka_conf_set(conf, "security.protocol", sp, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.ssl.certificate.verification",
                          cfg->kafka_enable_ssl_certificate_verification ?: "true", errstr, sizeof(errstr));
    }
    bool is_ssl = (sp && (strcmp(sp, "SSL") == 0 || strcmp(sp, "SASL_SSL") == 0));
    if (is_ssl)
    {
        if (cfg->kafka_ssl_cipher_suites && strlen(cfg->kafka_ssl_cipher_suites) > 0) rd_kafka_conf_set(conf, "ssl.cipher.suites", cfg->kafka_ssl_cipher_suites, errstr, sizeof(errstr));
        if (cfg->kafka_ssl_key_location && strlen(cfg->kafka_ssl_key_location) > 0) rd_kafka_conf_set(conf, "ssl.key.location", cfg->kafka_ssl_key_location, errstr, sizeof(errstr));
        if (cfg->kafka_ssl_key_password && strlen(cfg->kafka_ssl_key_password) > 0) rd_kafka_conf_set(conf, "ssl.key.password", cfg->kafka_ssl_key_password, errstr, sizeof(errstr));
        if (cfg->kafka_ssl_certificate_location && strlen(cfg->kafka_ssl_certificate_location) > 0) rd_kafka_conf_set(conf, "ssl.certificate.location", cfg->kafka_ssl_certificate_location, errstr, sizeof(errstr));
        if (cfg->kafka_ssl_ca_location && strlen(cfg->kafka_ssl_ca_location) > 0) rd_kafka_conf_set(conf, "ssl.ca.location", cfg->kafka_ssl_ca_location, errstr, sizeof(errstr));
        if (cfg->kafka_ssl_endpoint_identification_algorithm && strlen(cfg->kafka_ssl_endpoint_identification_algorithm) > 0) rd_kafka_conf_set(conf, "ssl.endpoint.identification.algorithm", cfg->kafka_ssl_endpoint_identification_algorithm, errstr, sizeof(errstr));
    }
    bool is_sasl = (sp && (strcmp(sp, "SASL_PLAINTEXT") == 0 || strcmp(sp, "SASL_SSL") == 0));
    if (is_sasl)
    {
        char *mech = cfg->kafka_sasl_mechanism ?: "plain";
        rd_kafka_conf_set(conf, "sasl.mechanism", mech, errstr, sizeof(errstr));
        if (strcmp(mech, "PLAIN") == 0 || strncmp(mech, "SCRAM", 5) == 0)
        {
            if (cfg->kafka_sasl_username && strlen(cfg->kafka_sasl_username) > 0) rd_kafka_conf_set(conf, "sasl.username", cfg->kafka_sasl_username, errstr, sizeof(errstr));
            if (cfg->kafka_sasl_password && strlen(cfg->kafka_sasl_password) > 0) rd_kafka_conf_set(conf, "sasl.password", cfg->kafka_sasl_password, errstr, sizeof(errstr));
        }
    }
    rd_kafka_conf_set(conf, "client.id", cfg->kafka_client_id ?: "pg-migrate-producer", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "log_level", cfg->kafka_log_level ?: "2", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "debug", cfg->kafka_debug ?: "", errstr, sizeof(errstr));

    g_rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!g_rk) { rd_kafka_conf_destroy(conf); return -1; }

    g_rkt = rd_kafka_topic_new(g_rk, task_name, NULL);
    if (!g_rkt) { rd_kafka_destroy(g_rk); g_rk = NULL; return -1; }
    return 0;
}

static void kafka_send_optimized(const char *msg, int msg_len)
{
    if (!g_rk || !g_rkt || !msg) return;
    long long sz = msg_len + 100;
    if (message_count >= max_message_count || (local_memory_usage + sz) >= ((long long)max_memory_kbytes * 1024))
    { rd_kafka_flush(g_rk, 1000); message_count = 0; }
    int retries = 0; rd_kafka_resp_err_t err;
    do {
        err = rd_kafka_produce(g_rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, (void*)msg, msg_len, NULL,0,NULL);
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) { rd_kafka_flush(g_rk,2000); message_count=0; rd_kafka_poll(g_rk,100); if(++retries>10) return; }
        else if (err) return;
    } while(err == RD_KAFKA_RESP_ERR__QUEUE_FULL);
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) { message_count++; local_memory_usage += sz; if (message_count%10==0) rd_kafka_poll(g_rk,0); }
}

/* ===================================================================
 *  引擎回调
 * =================================================================== */
static const char* my_get_buffer_prefix(void) { return ""; }
typedef struct { int dummy; } WriteCtx;

static void* my_write_init(int tidnum, const char *task_name, int block_sock)
{
    (void)tidnum; (void)task_name; (void)block_sock;
    return calloc(1, sizeof(WriteCtx));
}

static int my_write_exec(int tidnum, void *ctx, const char *buffer, long int size)
{
    (void)tidnum; (void)ctx;
    kafka_send_optimized(buffer, (int)size);
    return 0;
}

static void my_write_fini(int tidnum, void *ctx) { (void)tidnum; free(ctx); }
static void* my_get_escape_conn(int tidnum, void *write_ctx) { (void)tidnum; (void)write_ctx; return NULL; }

static int my_gen_result(const PGresult *res, void *escape_conn, char **result_point)
{
    assert(res != NULL); (void)escape_conn;
    int nf = PQnfields(res), nt = PQntuples(res);
    if (nt <= 0 || nf <= 0) return 0;
    char *old = *result_point;
    for (int r = 0; r < nt; r++) {
        cJSON *obj = cJSON_CreateObject();
        for (int c = 0; c < nf; c++) {
            const char *cn = PQfname(res, c);
            if (PQgetisnull(res, r, c)) cJSON_AddNullToObject(obj, cn);
            else cJSON_AddStringToObject(obj, cn, PQgetvalue(res, r, c));
        }
        char *j = cJSON_PrintUnformatted(obj);
        if (j) { FastStrcat2(result_point, j); **result_point='\n'; (*result_point)++; **result_point='\0'; free(j); }
        cJSON_Delete(obj);
    }
    return (int)(*result_point - old);
}

/* ===================================================================
 *  入口函数
 * =================================================================== */
int pg_to_kafka(int acceptSockfd)
{
    MyTaskCfg cfg;
    memset(&cfg, 0, sizeof(cfg));

    cfg.task_name            =       g_strdup(get_cfg("task_name") ?: "");
    cfg.task_speed           =       g_strdup(get_cfg("task_speed") ?: "1000");
    cfg.migrate_type         =       g_strdup(get_cfg("migrate_type") ?: "pg_to_kafka");
    cfg.pump_sql             =       g_strdup(get_cfg("pump_sql") ?: "");
    cfg.parallel_thread_per_task = atoi(get_cfg("parallel_thread_per_task") ?: "4");
    cfg.send_buffer_size     =       atoi(get_cfg("send_buffer_size") ?: "1024");
    cfg.source_host          =       g_strdup(get_cfg("source_host") ?: "");
    cfg.source_port          =       g_strdup(get_cfg("source_port") ?: "5432");
    cfg.source_dbname        =       g_strdup(get_cfg("source_dbname") ?: "");
    cfg.source_password      =       g_strdup(get_cfg("source_password") ?: "");
    cfg.source_username      =       g_strdup(get_cfg("source_username") ?: "");

    cfg.kafka_server                        = g_strdup(get_cfg("kafka_server") ?: "");
    cfg.kafka_queue_buffering_max_messages   = g_strdup(get_cfg("kafka_queue_buffering_max_messages") ?: "100000");
    cfg.kafka_queue_buffering_max_kbytes     = g_strdup(get_cfg("kafka_queue_buffering_max_kbytes") ?: "1048576");
    cfg.kafka_batch_num_messages             = g_strdup(get_cfg("kafka_batch_num_messages") ?: "10000");
    cfg.kafka_queue_enqueue_timeout_ms       = g_strdup(get_cfg("kafka_queue_enqueue_timeout_ms") ?: "5");
    cfg.kafka_message_timeout_ms             = g_strdup(get_cfg("kafka_message_timeout_ms") ?: "300000");
    cfg.kafka_retries                        = g_strdup(get_cfg("kafka_retries") ?: "2");
    cfg.kafka_retry_backoff_ms               = g_strdup(get_cfg("kafka_retry_backoff_ms") ?: "100");
    cfg.kafka_socket_send_buffer_bytes       = g_strdup(get_cfg("kafka_socket_send_buffer_bytes") ?: "65536");
    cfg.kafka_socket_receive_buffer_bytes    = g_strdup(get_cfg("kafka_socket_receive_buffer_bytes") ?: "65536");
    cfg.kafka_socket_connection_setup_timeout_ms = g_strdup(get_cfg("kafka_socket_connection_setup_timeout_ms") ?: "10000");
    cfg.kafka_metadata_max_age_ms            = g_strdup(get_cfg("kafka_metadata_max_age_ms") ?: "300000");
    cfg.kafka_compression_codec              = g_strdup(get_cfg("kafka_compression_codec") ?: "none");
    cfg.kafka_compression_level              = g_strdup(get_cfg("kafka_compression_level") ?: "-1");
    cfg.kafka_security_protocol              = g_strdup(get_cfg("kafka_security_protocol") ?: "plaintext");
    cfg.kafka_enable_ssl_certificate_verification = g_strdup(get_cfg("kafka_enable_ssl_certificate_verification") ?: "true");
    cfg.kafka_ssl_cipher_suites              = g_strdup(get_cfg("kafka_ssl_cipher_suites") ?: NULL);
    cfg.kafka_ssl_key_location               = g_strdup(get_cfg("kafka_ssl_key_location") ?: NULL);
    cfg.kafka_ssl_key_password               = g_strdup(get_cfg("kafka_ssl_key_password") ?: NULL);
    cfg.kafka_ssl_certificate_location       = g_strdup(get_cfg("kafka_ssl_certificate_location") ?: NULL);
    cfg.kafka_ssl_ca_location                = g_strdup(get_cfg("kafka_ssl_ca_location") ?: NULL);
    cfg.kafka_ssl_endpoint_identification_algorithm = g_strdup(get_cfg("kafka_ssl_endpoint_identification_algorithm") ?: "https");
    cfg.kafka_sasl_mechanism                 = g_strdup(get_cfg("kafka_sasl_mechanism") ?: "plain");
    cfg.kafka_sasl_username                  = g_strdup(get_cfg("kafka_sasl_username") ?: NULL);
    cfg.kafka_sasl_password                  = g_strdup(get_cfg("kafka_sasl_password") ?: NULL);
    cfg.kafka_client_id                      = g_strdup(get_cfg("kafka_client_id") ?: "pg-migrate-producer");
    cfg.kafka_log_level                      = g_strdup(get_cfg("kafka_log_level") ?: "2");
    cfg.kafka_debug                          = g_strdup(get_cfg("kafka_debug") ?: "");

    char source_conninfo[4096] = {0};
    snprintf(source_conninfo, sizeof(source_conninfo),
             "host=%s port=%s dbname=%s user=%s password=%s",
             cfg.source_host, cfg.source_port, cfg.source_dbname,
             cfg.source_username, cfg.source_password);

    if (kafka_init(&cfg, cfg.task_name) == 0)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "task_name:%s kafka init success\n", cfg.task_name ?: "unknown");
        ReplyToClient(acceptSockfd, msg);
        g_kafka_initialized = 1;
    }
    else
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "task_name:%s kafka init failed\n", cfg.task_name ?: "unknown");
        ReplyToClient(acceptSockfd, msg);
        my_task_free(&cfg);
        return -1;
    }

    PgEngineCallbacks cb;
    memset(&cb, 0, sizeof(cb));
    cb.gen_result          = my_gen_result;
    cb.get_escape_conn     = my_get_escape_conn;
    cb.write_init          = my_write_init;
    cb.write_exec          = my_write_exec;
    cb.write_fini          = my_write_fini;
    cb.get_buffer_prefix   = my_get_buffer_prefix;
    cb.page_size            = 16384;
    cb.speed_multiplier     = 1;
    cb.use_half_buffer_check = 1;

    int ret = pg_engine_run(acceptSockfd,
                            cfg.task_name, cfg.pump_sql,
                            cfg.parallel_thread_per_task, cfg.send_buffer_size,
                            source_conninfo, &cb);

    kafka_close();
    my_task_free(&cfg);
    return ret;
}