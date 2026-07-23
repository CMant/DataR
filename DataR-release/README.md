# DataR：地表最强数据同步工具

> **比DataX更快，更小，更灵活的ETL迁移工具**
<img width="558" height="546" alt="1111" src="https://github.com/user-attachments/assets/b538b76c-06e8-4d22-9f9e-19cb550343e8" />



# 项目说明  
> 目前支持pg_to_pg,pg_to_mysql,pg_to_file 3种方式。纯C实现，它不是实时cdc。是需要你向DataR发送 select a,b,c from table_name where a=1 ... 之类的sql语句进行迁移的工具。
使用pg连接协议的国产数据一样支持，比如kingbase。

# 核心特性：
- **极致性能**：纯 C + libpq 接口实现，Single-row 模式抽取。在单核上极限性能超越 DataX 至少一倍；同等硬件下（如12C服务器），DataX 仅能承载 1 个任务时，DataR 可承载 12 个任务。
- **动态调速**：支持运行时，实时调整迁移速度。可根据生产环境 IOPS/CPU 负载动态加减速，避免 ETL 干扰线上业务。
- **零扰动设计**：采用命名游标+单线程读取，无需 `COUNT(*)` 统计行数，任务启动立即返回，不对源库产生瞬时大压力。
- **内存安全高效**：基于缓冲区大小（而非行数）作为写入确认点，避免变长行导致的频繁内存申请/释放。手动内存管理 + O(1) 复杂度操作，运行期无 realloc/memset。
- **广泛兼容**：支持 PostgreSQL向PostgreSQL、MySQL、文件导出。兼容使用 PG 协议的国产数据库（如 Kingbase）。
- **极简架构**：`DataR`仅专注于“数据搬运”，无任务分发/权限管理等周边功能，它仅承担`如何最快地转换数据并插入`任务。不过，提供的 Socket 命令接口可以让你方便地扩展其他功能。
- **插件化扩展**：支持自定义迁移逻辑，编译为 `.so` 放入 lib 目录即可通过 `migrate_type` 调用。

# 使用方法
在pod镜像中已经提供了使用说明和脚本示例
  1.  start.py  开启一个任务
  2.  adj_speed.py  调整任务速度
  3.  delete.py   根据返回的task_pid 去kill 任务  


# start.py:
>  使用什么语言编写都可以，只要向DataR发送task_lines 中的任务字符串即可。migrate_type 修改数据迁移方式，比如pg_to_pg pg_to_mysql pg_to_file
>  你也可以编写自己的迁移方式，将它编译成.so 放到lib中。然后migrate_type 中填写你主函数的名称即可调用。
>  任务的管理时以task_name 做区分的，在内部构建了任务链表。在开启任务时，要保证任务的名称不重复。
```python
 task_lines = [
    "--create",
    "[DATAR]",
    "task_name=topic1",
    "source_host=192.168.227.128",
    "source_dbname=test",
    "source_port=54321",
    "source_username=system",
    "source_password=111111",
    "dest_host=192.168.227.128",
    "dest_port=54321",
    "dest_username=system",
    "dest_password=111111",
    "dest_dbname=test",
    "dest_table=tast_table1",
    "pump_sql=select * from test_table2",
    "task_speed=0",
    "parallel_thread_per_task=10",
    "send_buffer_size=64",
    "migrate_type=pg_to_pg"
]            
             
addtask = "\n".join(task_lines)
         
print("==== 发送任务 ====\n", addtask)
c.send(addtask.encode('utf-8'))
             
print("\n==== 等待服务端回复 ====\n")
while True:  
    recv_data = c.recv(2048) 
    if not recv_data:         
        print("\n✅ 服务端已断开连接，任务结束")
        break
             
    print(recv_data.decode('utf-8').strip())
             
c.close() 
```

# adjust_speed.py  
> 调整任务速度task_speed 越大越慢，=0时不限速。
```python
import socket
c = socket.socket()
host = '192.168.227.132'
port = 1234
c.connect((host, port))
task_lines = [
    "--adjust",
    "[DATAR]",
    "task_name=topic3",
    "task_speed=1000"
]
addtask = "\n".join(task_lines)
print("==== 发送任务 ====\n", addtask)
c.send(addtask.encode('utf-8'))
print("\n==== 等待服务端回复 ====\n")
while True:
    recv_data = c.recv(2048)  # 一直等消息
    if not recv_data:         # 服务端关闭连接才退出
        print("\n✅ 服务端已断开连接，任务结束")
        break
    
    print(recv_data.decode('utf-8').strip())

c.close()
```

注意：DataR仅仅是个核心程序，它只负责“尽快完成数据抽取、转换和写入”的任务，其他任务分发，权限管理等等都没有，这些功能很简单，我给DataR设计了几个命令接口，bash，python都能够完成。
可以做成ETL集群的模式，自己去写任务调动，节点注册登记，根据服务器负载自动派发任务。。。。这些都不难都不难的。
![调度中心图](https://github.com/user-attachments/assets/f7d48e9e-0f01-4d08-88aa-5fd4d5f27493)
![架构图](https://github.com/user-attachments/assets/d7ae6f33-79bb-461f-9cd8-ba4fd5f7aa06)

# DataX性能缺陷及DataR改进说明：
- **源端性能扰动**： DataX任务启动之初会使用count进行表行数统计，这个统计数据不是取自于数据字典，这样就会导致对线上生产环境造成性能扰动，比如产生大量IOPS，导致迁移任务产生延迟，而如果从数据字典中获取，数值并不是很准确，且为了支持更多种数据库，还需要针对每种数据库写单独的sql。  DataR的改进设计：命名游标的方式，单线程读取，任务启动立即返回。不需要统计行数，不对源节点产生性能扰动 。
- **一致性疑问**： 对DataX的事务一致性表示怀疑。它fetch数据方式为count之后把任务拆分给多个进程。这样是否会启动多个事务？多个事务并发启动，事务管理器按照队列分配事务ID，虽然大多数场景下不会产生数据不一致的问题，但极端情况下有这个可能，且确实属于设计缺陷。DataR的设计为单线程读取，只启动一个任务，只要回滚段够多，不存在事务不一致的问题。
- **一次内存申请**： DataX数据写入的确认点为“行数”，而表的行长是不固定的，这样就可能产生反复的内存申请和释放。DataR 数据写入的确认点为“缓冲区大小”，通过缓冲区判满（保留一部分偏移量防止出现移除）。使得缓冲区只需要在程序启动时申请，后续不需要反复扩缩。
- **语言优势**：DataR使用数据库的原生C API进行字符串转换（字段中存储的特殊字符需要进行处理，否则拼出的insert不正确）。手动内存管理+工程技巧，任务一旦开始，不需要recalloc，memeset，大多数操作复杂度保持在O(1)。
- **动态调速**：我在工作中发现，线上进行ETL任务会对生产环境性能造成干扰。DataX只能在任务开始之初设定一个固定的速度。而DataR则可以在任务进行的任何时间进行动态调速，这样就可以做一个类似TCP滑动窗口那样的功能进行拥塞控制，比如设定IOPS阈值超过70%，对ETL任务持续减速，IOPS小于20%时，可以适当增加迁移速度。这样既不浪费服务器资源，又能兼顾线上生产环境。
 


# 对比效果：  

- 极限性能超越DataX至少一倍，注意这是在单核心上达到的效果。语言优势带来的迁移速度效果是次要的，毕竟数据fetch的速度要远远低于代码运行的速度，但资源占用的优势确实是实打实的。CPU资源占用和功耗十分喜人。如果一台12c的服务器仅能承载1个DataX任务，那么可以承载12个DataR任务。个人估计如果使用在数据中心承担大批量的ETL任务，DataR可以比DataX每年节省几百万甚至上千万的费用且规模越大，节省的费用越多。
   

- 我的测试环境为三台pc机和一个千兆路由器，DataR跑满了千兆内网。生产环境上如果追求极限性能，注意网络瓶颈，建议做链路层聚合。
  

- 这部分为目标端写入时的服务器压力，可以看到DataX很少有超过6w行/s的，而DataR基本都在12w行/s以上，这部分只是对比极限迁移速度时DataR的优势，写入速度，网卡流量速度，都可以体现迁移速度。  


# DataX  

![图片3](https://github.com/user-attachments/assets/7a28b528-6240-4912-b288-96d6f8b3cf49)  


# DataR  

![图片4](https://github.com/user-attachments/assets/bbe16fef-7cc7-4531-9bd4-8a9850d6d238)  



# 其他指标对比：  

<img width="474" height="331" alt="图片5" src="https://github.com/user-attachments/assets/12fddf2b-0456-4e3c-9cc9-25d3b3a56342" />  



# taskspeed参数对磁盘性能的影响曲线。  

<img width="473" height="331" alt="图片6" src="https://github.com/user-attachments/assets/7944bd82-a515-4ab4-b018-527617438d08" />  



# 插座功耗对比DataR（单一任务对比）：  

# DataX  反转了，抱歉

![QQ截图20230517075652](https://github.com/user-attachments/assets/7ea38124-f525-4e23-ab89-a9bc9addaebc)  


# DataR:  

![微信图片_20230517080003](https://github.com/user-attachments/assets/33ad6eb9-f946-4716-b254-587d9a57c241)  




# R.ini是配置文件，支持center模式和standalone。
```ini
[DATAR]
DataR_node_name=pkgz
parallel_thread_per_task=10
#send_buffer_size=20480
# 这个单位在PG下为8KB的倍数，因为PG的默认页面大小为8KB。只要不包含toast字段，大多数情况下足够使用。在mysql下为16KB的倍数
# 缓冲区判定满的标志为剩余不到2个页面的大小。即： send_buffer_size*page_size-2*page_size
# 意外情况：toast长度超过2*page_size时，最后一次填充缓冲区可能会溢出。  
# 改成在控制台传递参数，每个任务可调

# buffer=32   max_t 70   63,829s/m
#最大的一个字段长度，默认1K，单位1k，使用者应当对所有字段的长度进行预估，避免出内存越界的风险，可适当大一些

#减速轮次，读取多少行数据之后减速一次。默认100
deceleration_rounds=10000
#迁移速度轮次，读取多少行数据之后进行一次速度统计，默认100000
calculate_speed_rounds=100000
#socket最大连接数，建议设置为cpu核心数
listen_backlog=50
#控制中心IP和端口
control_center_ip=
#control_center_ip=192.168.68.89
control_center_port=
#本机监听地址和端口
DataR_listener_ip=127.0.0.1
#接收缓冲区大小，防止--create 任务中的pump_sql 太长服务器无法接受。
recv_buffer_size=81920
DataR_listener_port=1234
```
> center：构建大规模的ETL服务集群，datarnode启动时需要向center进行登记注册，不填写center服务器无法启动。center对登记的服务器可以进行状态监控（center服务器很简单的，自己拿python写写就行啦。）
> standalone：自己就是个独立的ETL服务器，启动就能开始接收网络命令。


