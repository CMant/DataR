#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <glib.h>

#include <fcntl.h>
#include <sys/mman.h>
#include "include/common.h"
#include "include/read_from_db.h"
 
#include "include/message_type.h"
#include "include/ctl_command.h"
#include "include/adj_speed.h"
#include "include/network_service.h"
#define APP_NAME "DATAR"

extern char *DataR_node_name;
//extern int SendMessage_Center(char *message, char recvBuf[], int len);

char recv_center_ack[1024] = {0};

// 外部依赖声明
//extern char *trim(char *str);


 

static int is_shm_exist(const char *taskname){
    if (!taskname || strlen(taskname) == 0){
        return 0;
    }
        
       
    if(find_index(taskname)<0){
        return 0;
        
    }
    
    return 1;
}
//=====================================================
// 安全获取 glib 配置项 自动释放、判空、去空格
//=====================================================

// 全局哈希表，存放所有动态配置key-value
GHashTable *g_task_config = NULL;









//=====================================================
// 创建任务
//=====================================================
static int create_Task(const char *ctl_message,int acceptSockfd)
{

        if (!ctl_message)
    {
        fprintf(stderr, "[ERROR] ctl_message is NULL\n");
        return -1;
    }
    const char* cfg_data = ctl_message + 9;
    
    int ret = parse_config_text(cfg_data);
   
    if (ret != 0)
    {
        ReplyToClient(acceptSockfd,"[ERROR] 任务配置解析失败\n");
        return -1;
    }

    const char* task_name = get_cfg("task_name");
    if (!task_name || strlen(task_name) == 0)
    {
        fprintf(stderr, "[ERROR] task_name 配置缺失\n");
        ReplyToClient(acceptSockfd, "[ERROR] task_name 不能为空");
        return -1;
    }
    if (is_shm_exist(task_name))
    {
        fprintf(stderr, "[ERROR] 任务 %s 已存在\n", task_name);
        ReplyToClient(acceptSockfd, "[ERROR] 同名任务已存在");
        return -1;
    }

    // TASK_INF task_info = {0};
    // fill_task_info(&task_info);
    // printf("%d\n",atoi(task_info.task_speed));
    // exit(0);




    pid_t process_pid=getpid();
    // 设置调速
    int speed_val = atoi(get_cfg("task_speed"));
    //Set_sleepValue(speed_val, get_cfg("task_name"));
    fast_table_set_pid(get_cfg("task_name"), speed_val,process_pid);    
    char responsepid[16]  = {0};
    char START_MESSAGE[1024] = {0};
    snprintf(responsepid, sizeof(responsepid), "%d", process_pid);
    snprintf(START_MESSAGE, sizeof(START_MESSAGE), "%s_%s_%s_%s\n",
             TASK_CREATE_OK, get_cfg("task_name"), responsepid, DataR_node_name);
    ReplyToClient(acceptSockfd,START_MESSAGE);    
   

    
    if (0 == Read_from_DB(acceptSockfd))
    {
        
        //snprintf(replay_message, 1024, "%s_%s_%s_%s",task_info.task_name, responsepid, DataR_node_name);
        //Del_sleep_tag(task_info.task_name);
        fast_table_del( get_cfg("task_name"),process_pid);
     
    
    }
    else
    {
        
        ReplyToClient(acceptSockfd,"核心函数运行错误！\n");  
        return -1;
    }
    printf("%s任务结束。\n", get_cfg("task_name"));
    //clean:
    //g_key_file_free(cfg);
    return 0;
}


static char* safe_get_config_str(GKeyFile *cfg, const char *key)
{
    if (!cfg || !key) {
        return NULL;
    }

    // 从配置文件读取（glib 分配内存）
    gchar *raw = g_key_file_get_string(cfg, APP_NAME, key, NULL);
    if (!raw) {
        return NULL;
    }

    // 【重点】我们不释放 raw！让 trim 原地处理
    char *out = trim(raw);

    // 空判断
    if (!out || *out == '\0') {
        g_free(raw);
        return NULL;
    }

    // 【关键】复制一份安全字符串给外部使用
    char *ret = strdup(out);

    // 现在可以安全释放 glib 的内存了
    g_free(raw);
     if (0 == memcmp(key, "dest_username", 13) || 0 == memcmp(key, "dest_password", 13) || 0 == memcmp(key, "source_username", 15)||0 == memcmp(key, "source_password", 15)){
        printf("配置读取成功: [%s] = [%s]\n", key, "不许看");
    } 
    else {
        printf("配置读取成功: [%s] = [%s]\n", key, ret);
    }
    
    
    return ret; // 返回安全内存，调用者必须 free
}

//=====================================================
// 销毁任务
//=====================================================
static int delete_Task(const char *ctl_message,int acceptSockfd)
{   
    if (!ctl_message)
    {
        return -1;
    }
    ctl_message += 9;

    GError *err = NULL;
    GKeyFile *cfg = g_key_file_new();
    if (!g_key_file_load_from_data(cfg, ctl_message, -1, G_KEY_FILE_NONE, &err))
    {
        g_key_file_free(cfg);
        return -1;
    }

    char *task_name = safe_get_config_str(cfg, "task_name");
    char *task_pid = safe_get_config_str(cfg, "task_pid");
    if (!task_name)     { ReplyToClient(acceptSockfd,"task_name 缺失\n"); goto clean; }
    if (!task_pid )        { ReplyToClient(acceptSockfd,"task_pid 缺失\n"); goto clean; } 

    if (!is_shm_exist(task_name))
        {
            //printf("错误，任务不存在");
            ReplyToClient(acceptSockfd,"错误，任务不存在\n");
            return -1;
        }
    //Del_sleep_tag(task_name);
    if(fast_table_del(task_name,atoi(task_pid))<0){
        //fprintf(stderr,"fast_table_del执行出现错误,task_name和task_pid不匹配！\n");
        ReplyToClient(acceptSockfd,"fast_table_del执行出现错误,task_name和task_pid不匹配！\n");
        return -1;
    }else{

    pid_t kill_pid = atoi(task_pid);
    if (kill_pid <= 0)
    {
        perror("kill进程失败\n");
        ReplyToClient(acceptSockfd,"kill进程失败\n");
        return -1;
    }

    char DELETE_MESSAGE[1024] = {0};
    if (0 == kill(kill_pid, SIGKILL))
    {
        snprintf(DELETE_MESSAGE, sizeof(DELETE_MESSAGE), "%s|%s|%s",
                 TASK_DELETE_OK, ctl_message, DataR_node_name);
         ReplyToClient(acceptSockfd,DELETE_MESSAGE);        
    }
    else
    {
        snprintf(DELETE_MESSAGE, sizeof(DELETE_MESSAGE), "%s|%s|%s",
                 TASK_DELETE_FAILED, ctl_message, DataR_node_name);
         ReplyToClient(acceptSockfd,DELETE_MESSAGE);     
         return -1;   
    }


    }


    
     printf("%s任务已删除。\n",task_name);
   clean:
    g_key_file_free(cfg);
    return 0;
    
    
   
}

//=====================================================
// 节点心跳
//=====================================================
static int center_Hello(const char *ctl_message)
{
    (void)ctl_message;
    char HELLO_MESSAGE[64] = {0};
    snprintf(HELLO_MESSAGE, sizeof(HELLO_MESSAGE), "NODE_HELLO_%s", DataR_node_name);
 
    return 0;
}

//=====================================================
// 动态调速
//=====================================================
static int adjust_Speed(const char *ctl_message,int acceptSockfd)
{
    if (!ctl_message)
    {
        return -1;
    }
    ctl_message += 9;

    GError *err = NULL;
    GKeyFile *cfg = g_key_file_new();
    if (!g_key_file_load_from_data(cfg, ctl_message, -1, G_KEY_FILE_NONE, &err))
    {
        g_key_file_free(cfg);
        return -1;
    }

    char *task_name = safe_get_config_str(cfg, "task_name");
    char *task_speed = safe_get_config_str(cfg, "task_speed");
    if (!task_name)     { ReplyToClient(acceptSockfd,"task_name 缺失\n"); goto clean; }
    if (!task_speed )        { ReplyToClient(acceptSockfd,"task_speed 缺失\n"); goto clean; } 

    if (!is_shm_exist(task_name))
        {
            
            return -1;
        }
    char ADJUST_MESSAGE[1024] = {0};
    if (task_name && task_speed)
    {
        int sp = atoi(task_speed);
        if (sp<0){
            ReplyToClient(acceptSockfd,"task_speed不能小于0\n");
            return -1;
        }
        if (0 == fast_table_set(task_name, sp))
        {
            snprintf(ADJUST_MESSAGE, sizeof(ADJUST_MESSAGE), "%s%s_%ld",
                     TASK_ADJUST_OK, task_name, fast_table_get(task_name));
            ReplyToClient(acceptSockfd,ADJUST_MESSAGE);           
        }
        else
        {
            snprintf(ADJUST_MESSAGE, sizeof(ADJUST_MESSAGE), "%s%s_0",
                     TASK_ADJUST_FAILED, task_name);
            ReplyToClient(acceptSockfd,ADJUST_MESSAGE);  
            return -1;
        }
        printf("%s任务速度已调整为%d。\n",task_name,sp);
    }
    else
    {
        snprintf(ADJUST_MESSAGE, sizeof(ADJUST_MESSAGE), "%s", TASK_ADJUST_FAILED);
        ReplyToClient(acceptSockfd,ADJUST_MESSAGE);  
        return -1;
    }
    
clean:
    g_key_file_free(cfg);
   
 
    return 0;
}



//=====================================================
// 命令统一分发入口
//=====================================================
// ================= 修改函数签名：新增回复缓冲区 =================
int ctl_command_process(const char *ctl_message,int acceptSockfd)
{
     
    
    //const int replyBufLen=1024;
    //char replyBuf[replyBufLen];
    // 安全检查
    if (!ctl_message)
    {
       // snprintf(replyBuf, replyBufLen, "ERROR: 无效参数\n");
        ReplyToClient(acceptSockfd,"ERROR: 无效参数\n");
        return -1;
    }

    //printf("开始根据不同命令进行处理......\n");
    ReplyToClient(acceptSockfd,"开始根据不同命令进行处理......\n");

    size_t len_create  = strlen("--create");
    size_t len_delete  = strlen("--delete");
    size_t len_adjust  = strlen("--adjust");
    size_t len_hello   = strlen("--centerHello");

     //char replay_message[1024] = {0};
    // ================= 每个命令分支都生成对应的回复 =================
    if (0 == memcmp(ctl_message, "--create", len_create))
    {
    
        if(-1==create_Task(ctl_message,acceptSockfd)){
            //snprintf(replyBuf, replyBufLen, "ERROR: 任务已经存在，不要重复提交\n");
            ReplyToClient(acceptSockfd,"ERROR: 任务已经存在，不要重复提交\n");
            return -1;

        }else{
              //  snprintf(replyBuf, replyBufLen, "OK: 任务执行完毕%s\n",replay_message);
              ReplyToClient(acceptSockfd,"OK: 任务执行完毕\n");
              return 0;

        }
        



    }
    else if (0 == memcmp(ctl_message, "--delete", len_delete))
    {
        
        if(-1==delete_Task(ctl_message,acceptSockfd)){
             
            // snprintf(replyBuf, replyBufLen, "ERROR: 任务不存在，或者删除了错误的PID\n");
            ReplyToClient(acceptSockfd,"ERROR: 任务不存在，或者删除了错误的PID\n");
            return -1;
            
        }else{
            //snprintf(replyBuf, replyBufLen, "OK: 任务删除成功\n");
              //ReplyToClient(acceptSockfd,"OK: 任务删除成功\n");
              ReplyToClient(acceptSockfd,"OK: 任务删除成功\n");
              return 0;
        }
        
    }
    else if (0 == memcmp(ctl_message, "--adjust", len_adjust))
    {
        
        if(-1==adjust_Speed(ctl_message,acceptSockfd))
        {
            //snprintf(replyBuf, replyBufLen, "ERROR: 这个任务没有运行,不要调整不存在的任务\n");
            ReplyToClient(acceptSockfd,"ERROR: 这个任务没有运行,不要调整不存在的任务\n");
            return -1;
        }
        else{
           //snprintf(replyBuf, replyBufLen, "OK: 速度调整成功\n");
           // ReplyToClient(acceptSockfd,"OK: 速度调整成功\n");
           ReplyToClient(acceptSockfd,"OK: 速度调整成功\n");
           return 0;
        }
        
    }
    else if (0 == memcmp(ctl_message, "--centerHello", len_hello))
    {
        center_Hello(ctl_message);
       // snprintf(replyBuf, replyBufLen, "OK: 心跳响应成功\n");
    }
    else
    {
        // 未知命令也给回复
        //snprintf(replyBuf, replyBufLen, "ERROR: 未知命令\n");
        ReplyToClient(acceptSockfd, "ERROR: 未知命令\n");
         return -1;
    }
     
    return 0;
}