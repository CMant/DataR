#ifndef PG_ENGINE_H
#define PG_ENGINE_H

#include <libpq-fe.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 *  pg_engine — 通用 PG→任意目标 迁移引擎
 *
 *  将 pg_to_xxx.c 中完全相同的并发控制、信号量、PG 源端读取、
 *  速度控制、缓冲区管理、异步回复、清理等逻辑提取到此引擎中。
 *
 *  新增目标端时，只需实现 PgEngineCallbacks 中的回调函数，
 *  然后调用 pg_engine_run() 即可。
 *
 *  注意：TASK_INF 由各 pg_to_xx.c 自行定义，引擎不关心配置细节。
 *       引擎只需要 task_name, pump_sql, parallel_thread_per_task,
 *       send_buffer_size, source_conninfo 这几个参数。
 * ============================================================ */

/* ---------- 引擎回调接口 ---------- */

/* 每个回调的 tidnum 表示当前是第几个并发线程（0 ~ parallel-1） */

typedef struct {
    /* ----------------------------------------------------------
     * gen_result: 将 PGresult 中的一行/多行数据格式化为字符串，
     *             追加到 *result_point 指向的位置。
     *
     * @param res          PGresult（singleRow 模式下每次返回一行）
     * @param escape_conn  由 get_escape_conn 返回的转义连接句柄
     *                     （可为 NULL，由回调自行判断）
     * @param result_point 二级指针，指向当前缓冲区写入位置；
     *                     回调应在此追加数据并前移指针。
     * @return 写入的字节数（即 *result_point 前移的量）
     * ---------------------------------------------------------- */
    int (*gen_result)(const PGresult *res, void *escape_conn, char **result_point);

    /* ----------------------------------------------------------
     * get_escape_conn: 返回 gen_result 所需的转义连接句柄。
     *                  例如 PG 目标返回 PGconn*，MySQL 返回 MYSQL*，
     *                  文件/JSON 目标可返回 NULL。
     * ---------------------------------------------------------- */
    void* (*get_escape_conn)(int tidnum, void *write_ctx);

    /* ----------------------------------------------------------
     * write_init: 在写入线程中调用，用于初始化目标端连接/文件等。
     *
     * @param tidnum     线程编号
     * @param task_name  任务名称
     * @param block_sock 阻塞回复 socket（用于上报错误）
     * @return 写入上下文指针（write_ctx），后续传给 write_exec/write_fini
     * ---------------------------------------------------------- */
    void* (*write_init)(int tidnum, const char *task_name, int block_sock);

    /* ----------------------------------------------------------
     * write_exec: 将缓冲区中的数据写入目标端。
     *
     * @param tidnum  线程编号
     * @param ctx     write_init 返回的上下文
     * @param buffer  待写入数据
     * @param size    数据长度
     * @return 0=成功，非0=失败（引擎将终止迁移）
     * ---------------------------------------------------------- */
    int (*write_exec)(int tidnum, void *ctx, const char *buffer, long int size);

    /* ----------------------------------------------------------
     * write_fini: 写入线程结束时调用，用于释放目标端资源。
     * ---------------------------------------------------------- */
    void (*write_fini)(int tidnum, void *ctx);

    /* ----------------------------------------------------------
     * get_buffer_prefix: 返回每个缓冲区开头的固定前缀字符串。
     *                    例如 "insert into tab values "。
     *                    返回的字符串在引擎运行期间必须保持有效。
     * ---------------------------------------------------------- */
    const char* (*get_buffer_prefix)(void);

    /* ---------- 引擎行为配置 ---------- */
    int  page_size;            /* 用于缓冲区大小计算，如 8192 或 16384 */
    int  speed_multiplier;     /* usleep 乘数：0=不乘，1=乘1000 */
    int  use_half_buffer_check;/* 缓冲区满判断：1=用 current*2>=max，0=用 max-current<keep */
} PgEngineCallbacks;

/* ---------- 引擎入口 ---------- */

/**
 * pg_engine_run - 运行 PG→目标 数据迁移引擎
 *
 * @param acceptSockfd  客户端 socket 描述符
 * @param task_name     任务名称（用于回显和速度表查询）
 * @param pump_sql      源端查询 SQL
 * @param parallel_thread_per_task  并发线程数
 * @param send_buffer_size          发送缓冲区大小（单位：page_size 的倍数）
 * @param source_conninfo           PG 源端连接串 ("host=... port=... dbname=... user=... password=...")
 * @param cb            回调函数集（所有函数指针必须非 NULL）
 * @return 0=成功，-1=失败
 *
 * 引擎内部自动完成：
 *   1. 创建异步回复线程
 *   2. 分配信号量、缓冲区、写入线程
 *   3. 连接 PG 源端，以 singleRow 模式执行查询
 *   4. 主循环：读取 → gen_result → 速度控制 → 缓冲区满切换线程
 *   5. 清理：等待写入线程、释放资源
 */
int pg_engine_run(int acceptSockfd,
                  const char *task_name,
                  const char *pump_sql,
                  int parallel_thread_per_task,
                  int send_buffer_size,
                  const char *source_conninfo,
                  const PgEngineCallbacks *cb);

/* 源端 PG 连接，gen_result 回调中如需做 PQescapeStringConn 可直接使用 */
extern PGconn *pg_engine_source_conn;

#ifdef __cplusplus
}
#endif

#endif /* PG_ENGINE_H */