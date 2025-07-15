#include <sys/socket.h>
#include<stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <glib.h>
#include <fcntl.h>
#include <unistd.h>
#define APP_NAME "DATAR"
#define CONFIG_FILE "R.ini"
/***************文件全局变量*******************/
/***************以下是配置文件kv*******************/
char *sync_db_type;
int row_size;
int listen_backlog;
long long max_tuple_size;
int deceleration_rounds;
int calculate_speed_rounds;
char *control_center_ip;
long control_center_port;
char *DataR_listener_ip;
int DataR_listener_port;
char *auth_key;
char *kafka_bootstrap_servers;
int parallel_thread_per_task;
int send_buffer_size;
char *DataR_node_name;
int DataR_Mode=0;
extern int SendMessage_Center(char *message,char recvBuf[],int len);
/***************函数体********************/
//检查配置文件是否存在，可读
int config_file_check(char *config_filename){
	if((access(config_filename,F_OK))!=-1)   
	{   
		printf("配置文件 %s 存在.\n",config_filename);   
	}   
	else  
	{   
		perror("配置文件不存在.\n");
		exit(-1);  
	}   
	if(access(config_filename,R_OK)!=-1)   
	{   
		printf("配置文件 %s 可读.\n",config_filename);  
	}   
	else  
	{   
		perror("配置文件无权限读取.\n"); 
		exit(-1);    
	}   
	return 0;
}
//配置文件中的所有值进行初始化赋值
void config_value_init(char *config_filename){
	GKeyFile * config;
	config = g_key_file_new();
	g_key_file_load_from_file(config, config_filename, 0, NULL);
	// row_size=atoi(g_key_file_get_string(config,APP_NAME,"row_size",NULL));
	// if(row_size==0){
	// 	perror("row_size 未设置\n");
	// 	exit(-1);
	// }
	 
	listen_backlog=atoi(g_key_file_get_string(config,APP_NAME,"listen_backlog",NULL));
	if(listen_backlog==0){
		perror("listen_backlog 未设置\n");
		exit(-1);
	}
	control_center_ip=g_key_file_get_string(config,APP_NAME,"control_center_ip",NULL);
	//printf("%s   \n",control_center_ip);
	if(strlen(control_center_ip)==0){
		//control_center_ip="0";
		printf("control_center_ip 未设置,进入Standalone Mode \n");
		//exit(-1);
	}
	control_center_port=atol(g_key_file_get_string(config,APP_NAME,"control_center_port",NULL));
	//printf("%ld   \n",control_center_port);
	if(control_center_port==0){
		//control_center_ip=0;
		printf("control_center_port 未设置,进入Standalone Mode\n");
		//exit(-1);
	}
	DataR_listener_ip=g_key_file_get_string(config,APP_NAME,"DataR_listener_ip",NULL);
	if(strlen(DataR_listener_ip)==0){
		perror("DataR_listener_ip 未设置\n");
		exit(-1);
	}
	DataR_listener_port=atol(g_key_file_get_string(config,APP_NAME,"DataR_listener_port",NULL));
	if(DataR_listener_port==0){
		perror("DataR_listener_port 未设置\n");
		exit(-1);
	}
	// send_buffer_size=atoi(g_key_file_get_string(config,APP_NAME,"send_buffer_size",NULL));
	// if(send_buffer_size==0){
	// 	perror("send_buffer_size 未设置\n");
	// 	exit(-1);
	// }
	// 	max_tuple_size=atoll(g_key_file_get_string(config,APP_NAME,"max_tuple_size",NULL));
	// if(max_tuple_size==0){
	// 	perror("max_tuple_size 未设置\n");
	// 	exit(-1);
	// }
	
	deceleration_rounds=atoi(g_key_file_get_string(config,APP_NAME,"deceleration_rounds",NULL));
	if(deceleration_rounds==0){
		perror("deceleration_rounds 未设置\n");
		exit(-1);
	}
    
	calculate_speed_rounds=atoi(g_key_file_get_string(config,APP_NAME,"calculate_speed_rounds",NULL));
	if(calculate_speed_rounds==0){
		perror("calculate_speed_rounds 未设置\n");
		exit(-1);
	}

	// parallel_thread_per_task=atoi(g_key_file_get_string(config,APP_NAME,"parallel_thread_per_task",NULL));
	// if(parallel_thread_per_task==0){
	// 	perror("parallel_thread_per_task 未设置\n");
	// 	exit(-1);
	// }
	DataR_node_name=g_key_file_get_string(config,APP_NAME,"DataR_node_name",NULL);
	if(DataR_node_name==0){
		perror("DataR_node_name 未设置\n");
		exit(-1);
	}



 
	printf("control_center_ip           :%s\n",control_center_ip);
	printf("control_center_port         :%ld\n",control_center_port);
	printf("DataR_listener_ip           :%s\n",DataR_listener_ip);
	printf("DataR_listener_port         :%d\n",DataR_listener_port);
	printf("auth_key                    :%s\n",auth_key);
	// printf("row_size                    :%d\n",row_size);
	printf("listen_backlog              :%d\n",listen_backlog);
 
	// printf("send_buffer_size            :%d\n",send_buffer_size);
	// printf("max_tuple_size              :%lld\n",max_tuple_size);
	printf("deceleration_rounds         :%d\n",deceleration_rounds);
	printf("calculate_speed_rounds      :%d\n",calculate_speed_rounds);
	printf("DataR_node_name             :%s\n",DataR_node_name);

	g_key_file_free(config);
}
//初始化调度中心网络连接
int init_Send_to_Center(int *client_sockfd){
	int len = 0;
	struct sockaddr_in server_addr;
	char buf[1024] = {0};
	bzero(&server_addr, sizeof(struct sockaddr_in));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = inet_addr(control_center_ip);
	server_addr.sin_port = htons(control_center_port);
	//printf("fd %d",*client_sockfd);
	if((*client_sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		perror("socket error!\n");
		return -1;
	}
	if(connect(*client_sockfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr)) < 0)
	{
		perror("连接控制中心失败，请确认控制中心是否启动!\n");
		return -1;
	}
	printf("控制中心连接成功!\n");
	char LOGIN_MESSAGE[1024]={0};
	sprintf(LOGIN_MESSAGE,"%s %s","LOGIN IN",DataR_node_name);
	char recv_center_signal[1024] = {0};
	
	SendMessage_Center(LOGIN_MESSAGE,recv_center_signal,1024);
	
	printf("%s\n",recv_center_signal);
	if(0 == memcmp(recv_center_signal, "LOGIN OK",strlen("LOGIN OK"))){
		printf("接收到控制中心信号!\n");
		return 0;
	}
	else{
		perror("无法收到控制中心信号！,请检查配置\n");
		exit(-1);
	}
}
int check_center_connect(){
	//printf("%s    %ld\n",control_center_ip,control_center_port);
	if(strlen(control_center_ip)==0 || control_center_port==0 ){
		// control_center_ip="127.0.0.1";
		// control_center_port=DataR_listener_port;
		printf("Enter Standalone Mode\n");
		DataR_Mode=1;
	}
	else{
		printf("Enter Center Mode\n");
		DataR_Mode=0;
	} 
	

}
int init_check(){
	config_file_check(CONFIG_FILE);
	config_value_init(CONFIG_FILE);
    check_center_connect();
    return 0;
}
