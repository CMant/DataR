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
#include <inttypes.h>
#include <mysql.h>
#include <semaphore.h>
#include <time.h>
#include <sys/time.h>
#include <mysql.h>
#include "include/network_service.h"
#define PAGE_SIZE 16384
//-----------------------------------------------//
// FINAL
//-----------------------------------------------//
typedef struct task_para{
	int  tidnum;
	TASK_INF task_info;
	long int current_message_size;
	MYSQL   *dest_conn;//一个数据库链接指针 
 
	
	int finsh_tag;
	
}task_para;

static sem_t *sem_null;
static sem_t *sem_full;
static char **send_buffer;
static  int max_message_size;
static long int *sql_cmd_strlen;
static int KEEP_SIZE_BUFFER=4096*PAGE_SIZE;

// 异步回复客户端专用（只开1个线程）
static pthread_t g_reply_tid;
static int       unblock_g_reply_sock;    // 要发送的socket
static char      g_reply_msg[256];// 要发送的内容
static int       block_g_reply_sock;    // 要发送的socket

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


// extern int row_size;
// extern long long int send_buffer_size;
// extern int max_tuple_size;
// extern  int parallel_thread_per_task;
//最大字段长度缓冲区，默认4K，使用者应当对所有字段的长度进行预估，避免出内存越界的风险

//减速轮次，读取多少行数据之后减速一次。默认100
//static  int PQgetvalue_buffer_size;
extern int deceleration_rounds;
extern int calculate_speed_rounds;
//static int  Gen_Result(const PGresult *res,MYSQL *conn,char *send_buffer,char **Result_Point);
static PGconn * getConnection(char *conninfo);

static int  Gen_Result(const PGresult *res,MYSQL *conn,char **Result_Point)
{
	assert(NULL != res);
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
	for (cols = 0; cols < nfields; ++cols)
	{
		for (rows = 0; rows < ntuples; ++rows)
		{
		
			//Sql语句中，values 后面需要 ()
			//FastStrcat 大量字符串拼接比strcat效率更高，每次拼接不需要从头开始计算，从指针指向部分开始拼接即可。
			FastStrcat2(Result_Point,"(");
			for (cols = 0; cols <nfields; cols++)
			{
			char *mx=PQgetvalue(res, rows, cols);
			col_strsize=PQgetlength(res, rows, cols);
		 
						FastStrcat2(Result_Point,"'");
						Result_Point_size=mysql_real_escape_string_quote(conn,*Result_Point,mx,col_strsize,1);
						// tuple_size=tuple_size+Result_Point_size;
						 *Result_Point=*Result_Point+Result_Point_size;
						FastStrcat2(Result_Point,"',");
				
			}
			*Result_Point=*Result_Point-1;
			FastStrcat2(Result_Point,"),");
		}
	}


	return *Result_Point-old_Result_Point;
}

static void   *write_task(void *arg){
	task_para *tmp=(task_para *)arg;
	char replay_message[256];
	//printf("写入线程号 :%d\n",tmp->tidnum);
    int res;//执行sql语句后的返回标志
    
    tmp->dest_conn = mysql_real_connect(tmp->dest_conn,tmp->task_info.dest_host,tmp->task_info.dest_username,tmp->task_info.dest_password,tmp->task_info.dest_dbname,0,NULL,0);
    if (tmp->dest_conn) {
        //printf("写入任务连接成功!\n");
		snprintf(replay_message,256,"task_name:%s theard:%d connect_success\n",tmp->task_info.task_name,tmp->tidnum);
		ReplyToClient(block_g_reply_sock, replay_message);
    } else {
        //fprintf(stderr,"写入任务连接失败!\n");
		snprintf(replay_message,256,"task_name:%s thread:%d write_error\n",tmp->task_info.task_name,tmp->tidnum);
		ReplyToClient(block_g_reply_sock, replay_message);
        //fprintf(stderr,"%s\n",mysql_error(tmp->dest_conn));
		
     }

	
	 
	
	while(1){
		sem_wait(&sem_full[tmp->tidnum]);
		
		if (tmp->finsh_tag==1){
	
			break;

		}else{
		 res = mysql_real_query(tmp->dest_conn,send_buffer[tmp->tidnum],tmp->current_message_size);
	
         if(res) {
            //fprintf(stderr,"insert 数据出错");
			ReplyToClient(block_g_reply_sock, "insert ERROR\n");
			//fprintf(stderr,"%s\n",mysql_error(tmp->dest_conn));
		 	//fprintf(stderr,"%s 错误线程号: %d\n",send_buffer[tmp->tidnum] ,tmp->tidnum);
            mysql_close(tmp->dest_conn);
            exit(-1);
            } 
		
		sem_post(&sem_null[tmp->tidnum]);

		}

	}
	sem_post(&sem_null[tmp->tidnum]);
	mysql_close(tmp->dest_conn);
	return NULL;
}

static PGconn * getConnection(char *conninfo) {
	PGconn *conn = PQconnectdb(conninfo);
	printf("连接数据库成功\n");
	char REPORT_MESSAGE[1024]={0};
	if(PQstatus(conn) == CONNECTION_BAD)
	{	
		
		snprintf(REPORT_MESSAGE,1024,"连接到数据库失败，连接地址 %s\n",PQerrorMessage(conn));
		ReplyToClient(block_g_reply_sock, REPORT_MESSAGE);
		 
		exit(-1);
	}
	return conn;
}


int pg_to_mysql(TASK_INF task_info,int acceptSockfd){
	block_g_reply_sock=acceptSockfd;
    int parallel_thread_per_task=atoi(task_info.parallel_thread_per_task);
	//int sem_value;
	//int max_tuple_size=atoi(task_info.max_tuple_size);
	//int row_size=atoi(task_info.row_size);
	int send_buffer_size=atoi(task_info.send_buffer_size);
    char replay_message[256];
    //printf("%d 并发线程数				 :\n",parallel_thread_per_task);
	//printf("%d 最大的一个字段长度,单位1k  :\n",max_tuple_size);
	//printf("%d 最大行长度,单位1k         :\n",row_size);
	//printf("%d 发送缓冲区大小			 :\n",send_buffer_size);

	
    pthread_create(&g_reply_tid, NULL, reply_client_thread, NULL);
	

    char source_conninfo[4096]= {0};
	
	sprintf(source_conninfo,"host=%s port=%s dbname=%s user=%s password=%s",task_info.source_host,task_info.source_port,task_info.source_dbname,task_info.source_username,task_info.source_password);

	//time_t start_write_count;
	//time_t end_write_count=0;
	//PQgetvalue_buffer_size=max_tuple_size*1024;

	
	sem_null=(sem_t *)calloc(parallel_thread_per_task,sizeof(sem_t));
	sem_full=(sem_t *)calloc(parallel_thread_per_task,sizeof(sem_t));
	sql_cmd_strlen=(long int *)calloc(parallel_thread_per_task,sizeof(long int));
	task_para task_para1[parallel_thread_per_task]; 
	pthread_t tidW[parallel_thread_per_task] ;
	
	max_message_size=send_buffer_size*PAGE_SIZE+KEEP_SIZE_BUFFER;
	send_buffer=(char **)calloc(parallel_thread_per_task,sizeof(char*));
    
	for (int s=0;s<parallel_thread_per_task;s++){
      //  printf("max_message_size%d\n",max_message_size);
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
		task_para1[s].task_info=task_info;
		task_para1[s].dest_conn=mysql_init(NULL);
	    task_para1[s].finsh_tag=0;
		 
		pthread_create(&tidW[s], NULL, write_task, &task_para1[s]);
	}
	//缓冲区保留空间，避免出现意外，大小为两个行长度 
	
	/*tuplesize 用来统计一共往send_buffer中存放了多少数据。以免超过send_buffer的内存空间 */	
	/*PG读取部分，采用singleRow模式，保证对读取端的压力可控 */
	int sendstatus,singlemodestatus;
  
    
	PGconn *source_conn = getConnection(source_conninfo);
	PGresult *res;
	sendstatus=PQsendQuery(source_conn, task_info.pump_sql);
	singlemodestatus=PQsetSingleRowMode(source_conn);
	if(sendstatus!=1 && singlemodestatus!=1 ){
		fprintf(stderr,"single_MODE 配置失败\n");
	}
	/* 通过 percount_sleep 建立的循环，来控制在读取过程中Get_sleepValue的次数。
	*/
	//register int percount_sleep=0;
	/* Result_Point displayResult中用来拼接字符串用，FastStrcat会返回这次拼接字符串的位置，避免下次从头扫描*/
	char *Result_Point;
	int g=0;
	int sendbuffer_init_size=strlen(send_buffer[g]);
	int current_message_size=sendbuffer_init_size;
	int turn_i=0;
	int tuple_len=0;
	int dec=0;
	// clock_t  start_time, stop_time;
	struct timeval start_time,stop_time;
	// double duration_time;
	float duration_time;
	uint64_t row_count=0;

	MYSQL *dest_conn_escape_string;
	dest_conn_escape_string = mysql_init(NULL);
    dest_conn_escape_string = mysql_real_connect(dest_conn_escape_string,task_info.dest_host,task_info.dest_username,task_info.dest_password,task_info.dest_dbname,0,NULL,0);
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
					//恰好上一轮全部数据发送完毕，这里也要post？
					//不能post，新的缓冲区没数据，要么执行错误，要么重复执行
					task_para1[g].finsh_tag=1;
					 sem_post(&sem_full[g]);
					//printf("__NO RESULT_1\n");
					goto NO_RESULT;	
				}else{
					Result_Point=Result_Point-1;
					*Result_Point='\0';
					 task_para1[g].current_message_size=current_message_size-1;
					//sql_cmd_strlen[g]=current_message_size;
					//printf("__NO RESULT_2\n");
					sem_post(&sem_full[g]);

					goto NO_RESULT;	
				}
			}
			tuple_len=Gen_Result(res,dest_conn_escape_string,&Result_Point);
			if(tuple_len!=0){
				 
				++turn_i;
				++dec;
				++row_count;
				if(dec==deceleration_rounds){
					usleep(fast_table_get(task_info.task_name)*1000);
					dec=0;
				}	
				if(turn_i==calculate_speed_rounds){
					 gettimeofday(&stop_time,NULL);
					//  duration_time=1000000*(stop_time.tv_sec-start_time.tv_sec)+stop_time.tv_usec-start_time.tv_usec;
   					//  duration_time/=1000000;
					duration_time = stop_time.tv_sec - start_time.tv_sec;
					//printf("%s 当前迁移速度为%f行/s,可调速度参数为%ld \n",task_info.task_name,calculate_speed_rounds/duration_time,fast_table_get(task_info.task_name));
					snprintf(g_reply_msg, sizeof(g_reply_msg),
         			"task_name:%s %fr/s  task_speed:%ld row_count:%" PRIu64 "\n",
         			task_info.task_name,
         			calculate_speed_rounds / duration_time,
         			fast_table_get(task_info.task_name),
         			row_count);
					//snprintf(g_reply_msg, sizeof(g_reply_msg),  "task_name:%s %fr/s task_speed:%ld \n",task_info.task_name,calculate_speed_rounds/duration_time,fast_table_get(task_info.task_name));
					unblock_g_reply_sock = acceptSockfd;
					turn_i=0;
				}
			}
			current_message_size=current_message_size+tuple_len;
			if ((max_message_size-current_message_size)<KEEP_SIZE_BUFFER){ 
				 
				//发送缓冲区最后一个字符是逗号 ，  回退一个，重置成结束符
				Result_Point=Result_Point-1;
				*Result_Point='\0';
				task_para1[g].current_message_size=current_message_size-1;
				current_message_size=sendbuffer_init_size; 
			 	//printf("%s\n",send_buffer[g]);
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
