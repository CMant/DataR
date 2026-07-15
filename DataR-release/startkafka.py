import socket

# 创建客户端 socket
c = socket.socket()

# 服务端地址
host = '192.168.227.132'
port = 1234
c.connect((host, port))

# ===================== 任务配置（清爽易读版） =====================
task_lines = [
    "--create",
    "[DATAR]",
    "task_name=topic3",
    "task_speed=0",
    "migrate_type=pg_to_kafka",
    "pump_sql=select * from sbtest1",
    "parallel_thread_per_task=1",
    "send_buffer_size=1024",
    "source_host=192.168.227.132",
    "source_port=5432",
    "source_dbname=sbtest",
    "source_password=postgres",
    "source_username=postgres",
    "kafka_server=127.0.0.1:9092",
    "kafka_queue_buffering_max_messages=100000",
    "kafka_queue_buffering_max_kbytes=1048576",
    "kafka_batch_num_messages=10000",
    "kafka_queue_enqueue_timeout_ms=5",
    "kafka_message_timeout_ms=300000",
    "kafka_retries=2",
    "kafka_retry_backoff_ms=100",
    "kafka_socket_send_buffer_bytes=65536",
    "kafka_socket_receive_buffer_bytes=65536",
    "kafka_socket_connection_setup_timeout_ms=10000",
    "kafka_metadata_max_age_ms=300000",
    "kafka_compression_codec=none",
    "kafka_compression_level=-1",
    "kafka_security_protocol=plaintext",
    "kafka_enable_ssl_certificate_verification=true",
    "kafka_ssl_endpoint_identification_algorithm=https",
    "kafka_sasl_mechanism=plain",
    "kafka_client_id=pg-migrate-producer",
    "kafka_log_level=2",
    "kafka_debug="
]

addtask = "\n".join(task_lines)
# =================================================================

# 发送命令
print("==== 发送任务 ====\n", addtask)
c.send(addtask.encode('utf-8'))

# ===================== 循环接收，不退出！=====================
print("\n==== 等待服务端回复 ====\n")
while True:
    recv_data = c.recv(2048)  # 一直等消息
    if not recv_data:         # 服务端关闭连接才退出
        print("\n✅ 服务端已断开连接，任务结束")
        break
    
    print(recv_data.decode('utf-8').strip())

c.close()
