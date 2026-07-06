#define EXPORT __attribute__((visibility("default")))

#include <libpq-fe.h>
#include <assert.h>
#include <stdbool.h>
#include <unistd.h>
 
#include <stdlib.h>
#include <string.h>
#include "include/common.h"
#include "include/message_type.h"
#include "include/adj_speed.h"
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include "include/network_service.h"
#include <fcntl.h>
#include <inttypes.h>
#define PAGE_SIZE 8192;
//-----------------------------------------------//
// FINAL
//-----------------------------------------------//
typedef struct task_para{
	int  tidnum;
	char *dest_conninfo;
	char *task_name;
	long int current_message_size;
	int finish_tag;

	
	 
}task_para;

static sem_t *sem_null;
static sem_t *sem_full;
static char **send_buffer;
static  int max_message_size;


// 异步回复客户端专用（只开1个线程）
static pthread_t g_reply_tid;
static int       unblock_g_reply_sock;    // 要发送的socket
static char      g_reply_msg[256];// 要发送的内容
static int       block_g_reply_sock;    // 阻塞，要发送的socket，放在对速度要求不那么高，出错就需要立即退出的地方。

// 异步回复线程：永远循环，等待命令
void *reply_client_thread(void *arg)
{
	(void)arg;
    while (1)
    {
        // 没有消息就休眠（不占CPU）
        if (unblock_g_reply_sock <= 0)
        {
            usleep(1000); // 1ms
            continue;
        }

        // 有消息 → 执行回复
        ReplyToClient(unblock_g_reply_sock, g_reply_msg);

        // 执行完清空，等待下一次
        unblock_g_reply_sock = 0;
    }
    return NULL;
}

//减速轮次，读取多少行数据之后减速一次。默认100
//static  int PQgetvalue_buffer_size;
extern int deceleration_rounds;
extern int calculate_speed_rounds;

static PGconn * getConnection(char *conninfo);
static int  Gen_Result(const PGresult *res,PGconn *conn,char **Result_Point)
{
	assert(NULL != res && conn != NULL  && Result_Point != NULL);
	//行长统计
	//int tuple_size=0;
	int nfields = PQnfields(res);
	int ntuples = PQntuples(res);

	if(0 == ntuples || 0 == ntuples) { 
		return 0;
	}
	
	int rows = 0;
	int cols = 0;
	int col_strsize=0;
	int Result_Point_size=0;
	char *old_Result_Point=*Result_Point;


	for (rows = 0; rows < ntuples; ++rows)
	 
	{
		for (cols = 0; cols < nfields; ++cols)
		{

			//Sql语句中，values 后面需要 ()
			//FastStrcat 大量字符串拼接比strcat效率更高，每次拼接不需要从头开始计算，从指针指向部分开始拼接即可。
			FastStrcat2(Result_Point,"(");
			for (cols = 0; cols <nfields; cols++)
			{
				char *mx=PQgetvalue(res, rows, cols);
				col_strsize=PQgetlength(res, rows, cols);

				FastStrcat2(Result_Point,"'");

				
				Result_Point_size=PQescapeStringConn(conn,*Result_Point,mx,col_strsize,NULL);
		
				*Result_Point=*Result_Point+Result_Point_size;
				FastStrcat2(Result_Point,"',");

			}
			*Result_Point=*Result_Point-1;
			FastStrcat2(Result_Point,"),");
		}
	}
	return *Result_Point-old_Result_Point;
}

static PGconn * getConnection(char *conninfo) {
	PGconn *conn = PQconnectdb(conninfo);
	printf("连接数据库成功 !\n");
	char REPORT_MESSAGE[256]={0};
	if(PQstatus(conn) == CONNECTION_BAD)
	{	

		snprintf(REPORT_MESSAGE,256,"连接到数据库失败，连接地址 %s\n",PQerrorMessage(conn));
		ReplyToClient(block_g_reply_sock, REPORT_MESSAGE);
		exit(-1);
	}
	return conn;
}

void get_current_time(char *buf, int buf_len)
{
    time_t now = time(NULL);
    struct tm *tm = localtime(&now);

    // 格式化时间
    strftime(buf, buf_len, "%Y-%m-%d-%H-%M-%S", tm);
}

static void   *write_task(void *arg){
	task_para *tmp=(task_para *)arg;
	char replay_message[256];

    
	snprintf(replay_message,256,"线程%d写入任务连接成功\n",tmp->tidnum);
	ReplyToClient(block_g_reply_sock, replay_message);
    char filename[256];  // 足够大的栈数组（最快，不用malloc）
	//filename=tmp->tidnum;
	char time_buf[24];
	get_current_time(time_buf, 24);

    snprintf(filename, 256, "%s%s-%d-%s.sql", "sql_dump/",tmp->task_name, tmp->tidnum, time_buf);
	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    
    
	while(1){
		sem_wait(&sem_full[tmp->tidnum]);
		if (tmp->finish_tag==1){

			break;

		}else{


		write(fd, send_buffer[tmp->tidnum], tmp->current_message_size);
		write(fd, ";\n", 2);
	 
		sem_post(&sem_null[tmp->tidnum]);
		}

		}
	close(fd); 
	sem_post(&sem_null[tmp->tidnum]);
	return NULL;
}


EXPORT int pg_to_file(TASK_INF task_info,int acceptSockfd){
	block_g_reply_sock=	acceptSockfd;
	int parallel_thread_per_task=atoi(task_info.parallel_thread_per_task);
	//int sem_value;

	char replay_message[256];
	int send_buffer_size=atoi(task_info.send_buffer_size);
	//printf("并发线程数                :%d\n",parallel_thread_per_task);
	//printf("发送缓冲区大小            :%d\n",send_buffer_size);



	char source_conninfo[4096]= {0};
	char dest_conninfo[4096]= {0};
	pthread_create(&g_reply_tid, NULL, reply_client_thread, NULL);
	sprintf(source_conninfo,"host=%s port=%s dbname=%s user=%s password=%s",task_info.source_host,task_info.source_port,task_info.source_dbname,task_info.source_username,task_info.source_password);

	sprintf(dest_conninfo,"host=%s port=%s dbname=%s user=%s password=%s",task_info.dest_host,task_info.dest_port,task_info.dest_dbname,task_info.dest_username,task_info.dest_password);
	 

	 


	sem_null=(sem_t *)calloc(parallel_thread_per_task,sizeof(sem_t));
	sem_full=(sem_t *)calloc(parallel_thread_per_task,sizeof(sem_t));
	
	task_para task_para1[parallel_thread_per_task]; 
	pthread_t tidW[parallel_thread_per_task] ;
	max_message_size=(send_buffer_size+2)*PAGE_SIZE;
	send_buffer=(char **)calloc(parallel_thread_per_task,sizeof(char*));
	for (int s=0;s<parallel_thread_per_task;s++){
		send_buffer[s]=(char*)calloc(max_message_size,1);
		sprintf(send_buffer[s],"insert into %s values ",task_info.dest_table);
		if(send_buffer[s]==NULL){
			fprintf(stderr,"第 %d 个发送内存槽申请失败！请检查相关配置。\n",s);
			snprintf(replay_message,256,"第 %d 个发送内存槽申请失败！请检查相关配置。\n",s);
			ReplyToClient(acceptSockfd,replay_message);
			exit(-1);
		}
		sem_init(&sem_null[s],0,1);
		sem_init(&sem_full[s],0,0);
		task_para1[s].tidnum=s;
		task_para1[s].dest_conninfo=dest_conninfo;
		task_para1[s].task_name=task_info.task_name;
		task_para1[s].finish_tag=0;
		pthread_create(&tidW[s], NULL, write_task, &task_para1[s]);
	}
	//缓冲区保留空间，避免出现意外，大小为两个页面长度 
	int KEEP_SIZE_BUFFER=2*PAGE_SIZE;
	/*PG读取部分，采用singleRow模式，保证对读取端的压力可控 */
	int sendstatus,singlemodestatus;
	PGconn *source_conn = getConnection(source_conninfo);
	PGresult *res;
	sendstatus=PQsendQuery(source_conn, task_info.pump_sql);
	singlemodestatus=PQsetSingleRowMode(source_conn);
	if(sendstatus!=1 && singlemodestatus!=1 ){
		//fprintf(stderr,"single_MODE 配置失败\n");
		//snprintf(replay_message,256,"第 %d 个发送内存槽申请失败！请检查相关配置。\n",s);
		ReplyToClient(acceptSockfd,"single_MODE 配置失败\n");
	}

	/* Result_Point displayResult中用来拼接字符串用，FastStrcat会返回这次拼接字符串的位置，避免下次从头扫描*/
	char *Result_Point;
	int g=0;
	int sendbuffer_init_size=strlen(send_buffer[g]);
	int current_message_size=sendbuffer_init_size;
	int turn_i=0;
	int tuple_len=0;
	int dec=0;
	uint64_t row_count=0;
	
	struct timeval start_time,stop_time;

	float duration_time;

	while(1){
		sem_wait(&sem_null[g]);
		Result_Point=send_buffer[g]+sendbuffer_init_size;
		while(1){
			if(turn_i==0){
				// start_time=clock();
				gettimeofday(&start_time,NULL);  
			}
			res=PQgetResult(source_conn);
			if (NULL == res){
				if(current_message_size==sendbuffer_init_size){
					task_para1[g].finish_tag=1;
					sem_post(&sem_full[g]);
					//printf("__NO RESULT1\n");	
					goto NO_RESULT;	
				}else{
					Result_Point=Result_Point-1;
					*Result_Point='\0';
					task_para1[g].current_message_size=current_message_size-1;
					//printf("__NO RESULT_2\n");
					sem_post(&sem_full[g]);

					goto NO_RESULT;	
				}
			}
			tuple_len=Gen_Result(res,source_conn,&Result_Point);
			if(tuple_len!=0){
			
				++turn_i;
				++dec;
				++row_count;
				if(dec==deceleration_rounds){
				
					usleep(fast_table_get(task_info.task_name));
					dec=0;
				}	
				if(turn_i==calculate_speed_rounds){
					gettimeofday(&stop_time,NULL);
					// duration_time=1000000*(stop_time.tv_sec-start_time.tv_sec)+stop_time.tv_usec-start_time.tv_usec;
					// duration_time/=1000000;
					duration_time = stop_time.tv_sec - start_time.tv_sec;
					//printf("%s 当前迁移速度为%f行/s,可调速度参数为 %ld\n",task_info.task_name,calculate_speed_rounds/duration_time,fast_table_get(task_info.task_name));
						//snprintf(replay_message,256,"第 %d 个发送内存槽申请失败！请检查相关配置。\n",s);
					snprintf(g_reply_msg, sizeof(g_reply_msg),
         			"task_name:%s %fr/s  task_speed:%ld row_count:%" PRIu64 "\n",
         			task_info.task_name,
         			calculate_speed_rounds / duration_time,
         			fast_table_get(task_info.task_name),
         			row_count);
						//snprintf(g_reply_msg, sizeof(g_reply_msg),  "task_name: %s current: %fr/s  task_speed:%ld \n",task_info.task_name,calculate_speed_rounds/duration_time,fast_table_get(task_info.task_name));
					unblock_g_reply_sock = acceptSockfd;
					turn_i=0;
				}
			}
			current_message_size=current_message_size+tuple_len;
			//printf("xxxxx%dxxx%d\n",g,current_message_size);
			if ((max_message_size-current_message_size)<KEEP_SIZE_BUFFER){ 
				
				
				//发送缓冲区最后一个字符是逗号 ，  回退一个，重置成结束符
				Result_Point=Result_Point-1;
				*Result_Point='\0';
				task_para1[g].current_message_size=current_message_size-1;
				
				current_message_size=sendbuffer_init_size; 
	
				break;
			}
			PQclear(res);
		}
		sem_post(&sem_full[g]);
		g++;
		if(g==parallel_thread_per_task){
			g=0; 
		}
	}

NO_RESULT:
	
	sprintf(replay_message,"task_name:%s total: %ld rows\n",task_info.task_name,row_count);
	//snprintf(replay_message,256,"%s 共计迁移 %ld\n",task_info.task_name,row_count);
	ReplyToClient(acceptSockfd,replay_message);
	PQfinish(source_conn);
	for (int s=0;s<parallel_thread_per_task;s++){
 
			sem_wait(&sem_null[s]);

    	 
			free(send_buffer[s]);
			sem_destroy(&sem_null[s]);
			sem_destroy(&sem_full[s]);

 

	}
	free(sem_null);
	free(sem_full);
	free(send_buffer);
	 
	return 0;
}
