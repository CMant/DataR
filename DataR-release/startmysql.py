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
    "source_host=192.168.227.132",
    "source_dbname=sbtest",
    "source_port=5432",
    "source_username=postgres",
    "source_password=postgres",
    "dest_host=192.168.227.132",
    "dest_port=3306",
    "dest_username=root",
    "dest_password=111111",
    "dest_dbname=test",
    "dest_table=sbtest1",
    "pump_sql=select * from sbtest1",
    "task_speed=0",
    "parallel_thread_per_task=10",
    "send_buffer_size=512",
    "migrate_type=pg_to_kafka"
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
