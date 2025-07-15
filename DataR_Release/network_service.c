#include <sys/socket.h>
#include<stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include<string.h>
#include<fcntl.h>
#include "include/ctl_command.h"
#include "include/init_check.h"
#define handle_error(msg)\
	do { perror(msg); exit(EXIT_FAILURE); } while (0);
int send_to_center_sockfd;
extern int listen_backlog;
extern char *DataR_listener_ip;
extern long DataR_listener_port;
extern char *Auth_Key;
extern char *control_center_ip;
extern long control_center_port;
extern char *DataR_node_name;
extern int DataR_Mode;
int SendMessage_Center(char *message,char recvBuf[],int len);
void ServerProcess(void)
{
	int serverSockfd = -1;
	int acceptSockfd = -1;
	socklen_t addrLen = 0;
	char recvBuf[2048] = {0};
	char sendBuf[1024] = {0};
	int recvLen = 0;
	struct sockaddr_in tSocketServerAddr;
	struct sockaddr_in tSocketClientAddr;
	/* 调用 signal(SIGCHLD,SIG_IGN) 则子进程结束后，不再给父进程发送信号。
	   由内核负责回收，防止子进程变为僵尸进程 */
	signal(SIGCHLD, SIG_IGN);
	tSocketServerAddr.sin_family = AF_INET;
	tSocketServerAddr.sin_port = htons(DataR_listener_port);
	tSocketServerAddr.sin_addr.s_addr = htons(INADDR_ANY);
	/* 1. 创建socket */
	serverSockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (-1 == serverSockfd) {
		handle_error("socket");
	}
	/* 2. 绑定socket	 */
	if (-1 == bind(serverSockfd, (struct sockaddr *) &tSocketServerAddr, sizeof(struct sockaddr_in))) {
		handle_error("bind");
	}
	/* 3. 监听   */
	if (-1 == listen(serverSockfd, listen_backlog)) {
		handle_error("listen");
	}
    int Center_status=0;
	char recv_buf[64]={0};
	char login_message[64]={0};
	if (DataR_Mode==0){
		sprintf(login_message,"%s%s","NODE_LOGININ_",DataR_node_name);
		 while(SendMessage_Center(login_message,recv_buf,64)==-1){
           fprintf(stderr,"调度中心服务未启动\n");
            Center_status=Center_status+1;
			sleep(1);
			if(Center_status==5){
				 fprintf(stderr,"启动失败\n");
				 Center_status=0;
				 exit(-1);
			}
	}


	    printf("接收到控制中心信号：%s\n",recv_buf);
	}
	
	
	while (1) {
		addrLen = sizeof(struct sockaddr);
		acceptSockfd = accept(serverSockfd, (struct sockaddr *) &tSocketClientAddr, &addrLen);
		if (-1 != acceptSockfd) {
			printf("Connected Client IP : %s  \n", inet_ntoa(tSocketClientAddr.sin_addr));
			printf("Waiting client send message...\n");
			/* 创建子进程,用于多个client连接	*/
			if (0 == fork()) {
				while (1) {
					/* 阻塞接收客户端数据, 收到后返回,并打印 */
					recvLen = recv(acceptSockfd, recvBuf, 2047, 0);
					if (recvLen <= 0) {
						close(acceptSockfd);
						return;
					} else {
						recvBuf[recvLen] = '\0';
						ctl_command_process(recvBuf);
					}
				}
			}
		} else {
			handle_error("accept");
		}
	}
	close(send_to_center_sockfd);
	send_to_center_sockfd=-1;
	close(serverSockfd);
	serverSockfd = -1;

	
}

int SendMessage_Center(char *message,char recvBuf[],int len){
	if(DataR_Mode==0){

		int center_socket;
 
	struct sockaddr_in server_addr;

	bzero(&server_addr, sizeof(struct sockaddr_in));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = inet_addr(control_center_ip);
	server_addr.sin_port = htons(control_center_port);
	if((center_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		perror("socket error!\n");
		return -2;
	}	
	if(connect(center_socket, (struct sockaddr*)&server_addr, sizeof(struct sockaddr)) < 0)
	{
		perror("连接控制中心失败，请确认控制中心是否启动!\n");
		return -1;
	}

    send(center_socket,message, strlen(message), 0);
	int recvLen = recv(center_socket, recvBuf, len-1, 0);
	 
					if (recvLen <= 0) {
						close(center_socket);
						return 1;
					} else {
						recvBuf[recvLen] = '\0';
				 		return 0;
					}
    close(center_socket);

	}
	else{

		printf("%s\n",message);
	}
	
}