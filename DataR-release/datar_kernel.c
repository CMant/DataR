//核心迁移函数模板

#include "include/common.h"
#include "include/message_type.h"
#include "include/adj_speed.h"
#include "include/network_service.h"
#define PAGE_SIZE 16384;
//回复客户端信息的函数
ReplyToClient(client_socket_fd,msg);


//迁移多少行之后检查一次调速参数
extern int deceleration_rounds;
//迁移多少行之后计算速度
extern int calculate_speed_rounds;
//EXPORT 方式将函数暴露出来，编写成.so 放到lib中，migrate_type指定同名函数即可调用
#task_info为传递的任务参数，具体见头文件，acceptSockfd 客户端，使用上面的ReplyToClient回复消息。
EXPORT int pg_to_mysql(TASK_INF task_info,int acceptSockfd){

}