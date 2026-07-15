#ifndef KAFKA_H
#define KAFKA_H

#include <librdkafka/rdkafka.h>

// void kafka_init_copy(const char *brokers, const char *topic);
// void kafka_send_copy(char *msg);
void kafka_close_copy(rd_kafka_t **g_rk, rd_kafka_topic_t **g_rkt);

int kafka_init(rd_kafka_t **g_rk, rd_kafka_topic_t **g_rkt, TASK_INF );
void kafka_send(const char *msg, rd_kafka_t *g_rk, rd_kafka_topic_t *g_rkt);
void kafka_close(rd_kafka_t **g_rk, rd_kafka_topic_t **g_rkt);
void kafka_send_optimized(char *msg, rd_kafka_t *rk, rd_kafka_topic_t *rkt);
void get_dbname_from_conninfo(const char *conninfo, char *out_dbname, int max_len);

#endif