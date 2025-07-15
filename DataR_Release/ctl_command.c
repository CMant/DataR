#include <stdio.h>
#include "include/common.h"
#include <glib.h>
/* ===========postgresql=============================================================== */
#include "include/read_from_db.h"
/* ===========调速功能=============================================================== */
#include "include/speed_adj.h"
/* ===========其他功能类=============================================================== */

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "include/message_type.h"
#define APP_NAME "DATAR"
#include "include/ctl_command.h"
extern char *DataR_node_name;
char recv_center_ack[1024]={0};
//ctl内部函数处理，// 命令行参数处理
extern int SendMessage_Center(char *message,char recvBuf[],int len);
static int create_Task(char *ctl_message){
	ctl_message=ctl_message+9;

	GKeyFile * config;
	
	config = g_key_file_new();
	g_key_file_load_from_data(config,ctl_message,-1,G_KEY_FILE_NONE,NULL);
	/*读取配置参数内容*/ 
	

	TASK_INF task_info;
	task_info.migrate_type=trim(g_key_file_get_string(config,APP_NAME,"migrate_type",NULL));
	if(strlen(task_info.migrate_type)==0){
		fprintf(stderr,"migrate_type 未设置\n");
		exit(-1);
	} 
	task_info.task_name=trim(g_key_file_get_string(config,APP_NAME,"task_name",NULL));
	if(strlen(task_info.task_name)==0){
		fprintf(stderr,"task_name 未设置\n");
		exit(-1);
	} 
 
	task_info.source_host=trim(g_key_file_get_string(config,APP_NAME,"source_host",NULL));
	if(strlen(task_info.source_host)==0){
		fprintf(stderr,"source_host 未设置\n");
		exit(-1);
	} 
	task_info.source_port=trim(g_key_file_get_string(config,APP_NAME,"source_port",NULL));
	if(strlen(task_info.source_port)==0){
		fprintf(stderr,"source_port 未设置\n");
		exit(-1);
	} 
	task_info.source_dbname=trim(g_key_file_get_string(config,APP_NAME,"source_dbname",NULL));
	if(strlen(task_info.source_dbname)==0){
		fprintf(stderr,"source_dbname 未设置\n");
		exit(-1);
	} 
	task_info.source_username=trim(g_key_file_get_string(config,APP_NAME,"source_username",NULL));
	if(strlen(task_info.source_username)==0){
		fprintf(stderr,"source_username 未设置\n");
		exit(-1);
	} 
	task_info.source_password=trim(g_key_file_get_string(config,APP_NAME,"source_password",NULL));
	if(strlen(task_info.source_password)==0){
		fprintf(stderr,"source_password 未设置\n");
		exit(-1);
	} 
    task_info.dest_host=trim(g_key_file_get_string(config,APP_NAME,"dest_host",NULL));
	if(strlen(task_info.dest_host)==0){
		fprintf(stderr,"dest_host 未设置\n");
		exit(-1);
	} 
	task_info.dest_port=trim(g_key_file_get_string(config,APP_NAME,"dest_port",NULL));
	if(strlen(task_info.dest_port)==0){
		fprintf(stderr,"dest_port 未设置\n");
		exit(-1);
	} 
	task_info.dest_dbname=trim(g_key_file_get_string(config,APP_NAME,"dest_dbname",NULL));
	if(strlen(task_info.dest_dbname)==0){
		fprintf(stderr,"dest_dbname 未设置\n");
		exit(-1);
	} 
	task_info.dest_username=trim(g_key_file_get_string(config,APP_NAME,"dest_username",NULL));
	if(strlen(task_info.dest_username)==0){
		fprintf(stderr,"dest_username 未设置\n");
		exit(-1);
	} 
	task_info.dest_password=trim(g_key_file_get_string(config,APP_NAME,"dest_password",NULL));
	if(strlen(task_info.dest_password)==0){
		fprintf(stderr,"dest_password 未设置\n");
		exit(-1);
	} 
	task_info.dest_table=trim(g_key_file_get_string(config,APP_NAME,"dest_table",NULL));
	if(strlen(task_info.dest_table)==0){
		fprintf(stderr,"dest_table 未设置\n");
		exit(-1);
	} 
	task_info.task_speed=trim(g_key_file_get_string(config,APP_NAME,"task_speed",NULL));
	if(strlen(task_info.task_speed)==0){
		fprintf(stderr,"task_speed 未设置\n");
		exit(-1);
	} 
	// task_info.deceleration_rounds=trim(g_key_file_get_string(config,APP_NAME,"deceleration_rounds",NULL));
	// if(strlen(task_info.deceleration_rounds)==0){
	// 	fprintf(stderr,"deceleration_rounds 未设置\n");
	// 	exit(-1);
	// } 
	task_info.pump_sql=trim(g_key_file_get_string(config,APP_NAME,"pump_sql",NULL));
	if(strlen(task_info.pump_sql)==0){
		fprintf(stderr,"pump_sql 未设置\n");
		exit(-1);
	} 
	 
	task_info.row_size=trim(g_key_file_get_string(config,APP_NAME,"row_size",NULL)); 
	if(strlen(task_info.row_size)==0){
		fprintf(stderr,"row_size 未设置\n");
		exit(-1);
	} 
	task_info.parallel_thread_per_task=trim(g_key_file_get_string(config,APP_NAME,"parallel_thread_per_task",NULL)); 
	if(strlen(task_info.parallel_thread_per_task)==0){
		fprintf(stderr,"parallel_thread_per_task 未设置\n");
		exit(-1);
	} 
	task_info.send_buffer_size=trim(g_key_file_get_string(config,APP_NAME,"send_buffer_size",NULL)); 
	if(strlen(task_info.send_buffer_size)==0){
		fprintf(stderr,"send_buffer_size 未设置\n");
		exit(-1);
	} 
	task_info.max_tuple_size=trim(g_key_file_get_string(config,APP_NAME,"max_tuple_size",NULL)); 
	if(strlen(task_info.max_tuple_size)==0){
		fprintf(stderr,"max_tuple_size 未设置\n");
		exit(-1);
	} 




	/*生成相应连接信息*/ 
	printf("%s\n",task_info.pump_sql);
	char FINISH_MESSAGE[1024]= {0};
	char START_MESSAGE[1024]={0};
	// sprintf(source_conninfo,"host=%s port=%s dbname=%s user=%s password=%s",source_host,source_port,source_dbname,source_username,source_password);
	// sprintf(dest_conninfo,"host=%s port=%s dbname=%s user=%s password=%s",dest_host,dest_port,dest_dbname,dest_username,dest_password);
	Set_sleepValue(atoi(task_info.task_speed),task_info.task_name);
	char responsepid[16]={0};
	sprintf(responsepid, "%d", getpid());
	sprintf(START_MESSAGE,"%s%s_%s_%s",TASK_CREATE_OK,task_info.task_name,responsepid,DataR_node_name);

	SendMessage_Center(START_MESSAGE,recv_center_ack,1024);
	char result_num[64]={0};
	if(Read_from_DB(task_info,result_num)==0){
		sprintf(FINISH_MESSAGE,"%s%s_%s_%s_%s",TASK_FINISH,task_info.task_name,responsepid,DataR_node_name,result_num);
		SendMessage_Center(FINISH_MESSAGE,recv_center_ack,1024);
	}
	else{
		sprintf(FINISH_MESSAGE,"%s%s_%s_%s",TASK_CREATE_FAILED,task_info.task_name,responsepid,DataR_node_name);
		SendMessage_Center(FINISH_MESSAGE,recv_center_ack,1024);
	}
	g_key_file_free(config);
}
static int delete_Task(char *ctl_message){
	ctl_message=ctl_message+9;
	char DELETE_MESSAGE[1024]= {0};
 
	//ctl_message 需要转成int， 
	if(kill(atoi(ctl_message),SIGKILL)==0){
		sprintf(DELETE_MESSAGE,"%s|%s|%s",TASK_DELETE_OK,ctl_message,DataR_node_name);
		SendMessage_Center(DELETE_MESSAGE,recv_center_ack,1024);
		return 0;
	}
	else{
		sprintf(DELETE_MESSAGE,"%s|%s|%s",TASK_DELETE_FAILED,ctl_message,DataR_node_name);
		SendMessage_Center(DELETE_MESSAGE,recv_center_ack,1024);
		return 1;
	}
}

static int center_Hello(char *ctl_message){
	char HELLO_MESSAGE[64]={0};
	sprintf(HELLO_MESSAGE,"%s_%s_%s","NODE","HELLO",DataR_node_name);
	SendMessage_Center(HELLO_MESSAGE,recv_center_ack,64);
	return 0;
}

static int adjust_Speed(char *ctl_message){
	char ADJUST_MESSAGE[1024]= {0};
	ctl_message=ctl_message+9;
	GKeyFile * config;
	gchar *task_name;
	gchar *task_speed;
	config = g_key_file_new();
	g_key_file_load_from_data(config,ctl_message,-1,G_KEY_FILE_NONE,NULL);
	task_name=trim(g_key_file_get_string(config,APP_NAME,"task_name",NULL));
	task_speed=trim(g_key_file_get_string(config,APP_NAME,"task_speed",NULL));
	if(Set_sleepValue(atoi(task_speed),task_name)==0){
		sprintf(ADJUST_MESSAGE,"%s%s_%d",TASK_ADJUST_OK,task_name,Get_sleepValue(task_name));
		SendMessage_Center(ADJUST_MESSAGE,recv_center_ack,1024);
		g_key_file_free(config);
		return 0;
	} 
	else{
		sprintf(ADJUST_MESSAGE,"%s%s_%d",TASK_ADJUST_FAILED,task_name,0);
		SendMessage_Center(ADJUST_MESSAGE,recv_center_ack,1024);
		g_key_file_free(config);
		return 1;
	}
}

void ctl_command_process(char *ctl_message)
{
	printf("%s\n",ctl_message);
	if (0 == memcmp(ctl_message, "--create", strlen("--create"))) {
		
		create_Task(ctl_message);
	} else if (0 == memcmp(ctl_message, "--delete", strlen("--delete"))) {
		
		delete_Task(ctl_message);
	} 
	else if (0 == memcmp(ctl_message, "--adjust", strlen("--adjust"))){
		adjust_Speed(ctl_message);
	}
 
	else if (0 == memcmp(ctl_message, "--centerHello", strlen("--unfreeze"))){
		center_Hello(ctl_message);
	}
	else {
		//send(send_to_center_sockfd,TASK_ERROR, strlen(TASK_ERROR), 0);
	}
}
