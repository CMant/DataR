# DataR
<img width="558" height="546" alt="1111" src="https://github.com/user-attachments/assets/b538b76c-06e8-4d22-9f9e-19cb550343e8" />

#项目说明  
目前仅支持pg_to_pg,pg_to_mysql 两种方式。


DataX的目的是为了工作，而DataR的目的是为了干死DataX。

这个项目是我在读研期间完成的。我对计算机，操作系统和数据库的理解，基本就在这个项目上。
研究生毕业了，工作找不到，我就把这个项目开源了，核心算法部分做成了.so。不好意思，舍不得开出来。其他的都是一些零零碎碎的辅助功能，代码遍地是。

注意：DataR仅仅是个核心程序，它只负责“尽快完成数据抽取、转换和写入”的任务，其他任务分发，权限管理等等都没有，这些功能很简单，我给DataR设计了几个命令接口，bash，python都能够完成。

DataX性能缺陷及DataR改进说明：
1 DataX任务启动之初会使用count进行表行数统计，这个统计数据不是取自于数据字典，这样就会导致对线上生产环境造成性能扰动，比如产生大量IOPS，导致迁移任务产生延迟，而如果从数据字典中获取，数值并不是很准确，且为了支持更多种数据库，还需要针对每种数据库写单独的sql。
  DataR的改进设计：命名游标的方式，单线程读取，任务启动立即返回。不需要统计行数，不对源节点产生性能扰动 。
2 对DataX的事务一致性表示怀疑。它的fetch数据方式为count之后把任务拆分给多个进程。这样是否会启动多个事务？多个事务并发启动，事务管理器按照队列分配事务ID，虽然大多数场景下不会产生数据不一致的问题，但极端情况下有这个可能，且确实属于设计缺陷。
  DataR的设计为单线程读取，只启动一个任务，只要回滚段够多，不存在事务不一致的问题。
3 DataX数据写入的确认点为“行数”，而表的行长是不固定的，这样就可能产生反复的内存申请和释放。
  DataR 数据写入的确认点为“缓冲区大小”，通过缓冲区判满（保留一部分偏移量防止出现移除）。使得缓冲区只需要在程序启动时申请，后续不需要反复扩缩。
4 语言优势，DataR使用数据库的原生C API进行字符串转换（字段中存储的特殊字符需要进行处理，否则拼出的insert不正确）。手动内存管理+工程技巧，任务一旦开始，不需要recalloc，memeset，大多数操作复杂度保持在O(1)。
5 重点！！动态调速，我在工作中发现，线上进行ETL任务会对生产环境性能造成干扰。DataX只能在任务开始之初设定一个固定的速度。而DataR则可以在任务进行的任何时间进行动态调速，这样就可以做一个类似TCP滑动窗口那样的功能进行拥塞控制，比如设定IOPS阈值超过70%，对ETL任务持续减速，IOPS小于20%时，可以适当增加迁移速度。这样既不浪费服务器资源，又能兼顾线上生产环境。

  
以下为对比效果：
极限性能超越DataX至少一倍，注意这是在单核心上达到的效果。语言优势带来的迁移速度效果是次要的，毕竟数据fetch的速度要远远低于代码运行的速度，但资源占用的优势确实是实打实的。CPU资源占用和功耗十分喜人。如果一台12c的服务器仅能承载1个DataX任务，那么可以承载12个DataR任务。个人估计如果使用在数据中心承担大批量的ETL任务，DataR可以比DataX每年节省几百万甚至上千万的费用且规模越大，节省的费用越多。
这部分为目标端写入时的服务器压力，可以看到DataX很少有超过6w行/s的，而DataR基本都在12w行/s以上
DataX   
![图片3](https://github.com/user-attachments/assets/7a28b528-6240-4912-b288-96d6f8b3cf49)

DataR     
![图片4](https://github.com/user-attachments/assets/bbe16fef-7cc7-4531-9bd4-8a9850d6d238)


其他指标对比：
<img width="474" height="331" alt="图片5" src="https://github.com/user-attachments/assets/12fddf2b-0456-4e3c-9cc9-25d3b3a56342" />


taskspeed参数对磁盘性能的影响曲线。
<img width="473" height="331" alt="图片6" src="https://github.com/user-attachments/assets/7944bd82-a515-4ab4-b018-527617438d08" />


插座功耗对比DataR：
DataX
![QQ截图20230517075652](https://github.com/user-attachments/assets/7ea38124-f525-4e23-ab89-a9bc9addaebc)

DataR:
![微信图片_20230517080003](https://github.com/user-attachments/assets/33ad6eb9-f946-4716-b254-587d9a57c241)

#安装  

```bash
apt-get install libglib2.0-dev
apt-get install libpq-dev
#mysql包
wget https://repo.mysql.com//mysql-apt-config_0.8.34-1_all.deb
dpkg -i mysql-apt-config_0.8.34-1_all.deb
apt install libmysqlclient-dev
make
```
生成DataR文件直接运行即可。

DataR.ini是配置文件，支持center模式和standalone。
center：构建大规模的ETL服务集群，datarnode启动时需要向center进行登记注册，不填写center服务器无法启动。center对登记的服务器可以进行状态监控（center服务器很简单的，自己拿python写写就行啦。）
standalone：自己就是个独立的ETL服务器，启动就能开始接收网络命令。

发送任务：
测试模块，通过socket向DataR发送命令即可，你必须给这次迁移任务起个名字，taskname。调速，任务暂停，kill都需要根据这个任务名字来。
 ```python
import socket #导入socket 模块

c = socket.socket()  #创建socket对象

host = '192.168.174.128'  #设置DataR

port = 1234 #设置DataR接收端口

c.connect((host,port))

addtask="--create \n[DATAR]\n%s \n%s \n%s \n%s \n%s \n%s \n%s\n%s\n%s\n%s \n%s"%('taskname=topic1','sourcehost=192.168.1.68' ,'sourcedbname=postgres' ,\'sourceuser=postgres' ,'sourcepassword=postgres' ,'pumpsql=select * from M_table ','taskspeed=0', 'row_size=1','max_tuple_size=1','parallel_thread_per_task=10','send_buffer_size=20480')
c.send(addtask.encode('utf-8'))
recv_data=c.recv(2048)
print("xxxxxxx"+recv_data.decode('utf-8'))
c.close()
```

调速
```python
import socket #导入socket 模块
taskname='task_name=topic1'
taskspeed='1000000'
#taskspeed单位是ms，这样可调范围更多
adjspeed="--adjust\n[DATAR]\n%s\n%s"%(taskname,taskspeed)
#print(adjspeed)
c = socket.socket()  #创建socket对象
host = '192.168.1.3'  #设置本地主机
port = 1234 #设置端口号
c.connect((host,port))
c.send(adjspeed.encode('utf-8'))
recv_data=c.recv(2048)
print("xxxxxxx"+recv_data.decode('utf-8'))
c.close()
```
kill任务
创建任务的时候会返回一个进程号，你需要通过脚本对进程号进行记录，后面通过paramiko之类的包进行连接kill即可。
