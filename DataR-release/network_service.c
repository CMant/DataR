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
extern int recv_buffer_size;
// ================= 前置声明：方案二（异步回复）的函数签名 =================

int SendMessage_Center(char *message,char recvBuf[],int len);

// ================= 可以直接用！简单直接的回复客户端函数 =================
int ReplyToClient(int client_fd, const char *message) {
    if (client_fd < 0 || !message) {
        return -1;
    }
    
    int sent = send(client_fd, message, strlen(message), 0);
    if (sent == -1) {
        perror("❌ 回复客户端失败");
        return -1;
    }
    
    //printf("✅ 已回复客户端: %s\n", message);
    return 0;
}

void ServerProcess(void)
{
	int serverSockfd = -1;
	int acceptSockfd = -1;
	socklen_t addrLen = 0;
	char recvBuf[recv_buffer_size];
	memset(recvBuf,0,recv_buffer_size);
	int recvLen = 0;
	struct sockaddr_in tSocketServerAddr;
	struct sockaddr_in tSocketClientAddr;

	signal(SIGCHLD, SIG_IGN);

	tSocketServerAddr.sin_family = AF_INET;
	tSocketServerAddr.sin_port = htons(DataR_listener_port);
	tSocketServerAddr.sin_addr.s_addr = htons(INADDR_ANY);

	/* 1. 创建socket */
	serverSockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (-1 == serverSockfd) {
		handle_error("socket");
	}

	// ================================
	// 端口复用，解决 bind 地址占用
	// ================================
	int opt = 1;
	setsockopt(serverSockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

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

		if (0 == fork()) {
			// ================= 子进程：处理客户端 =================
			char replyBuf[2048] = {0};
			pid_t child_pid = getpid();
			char pid_reply[256];
			snprintf(pid_reply, sizeof(pid_reply), "WORKER_PID=%d\n", child_pid);
			ReplyToClient(acceptSockfd, pid_reply);

			// 只接收 一次 命令，处理，回复，然后关闭客户端
			memset(recvBuf, 0, recv_buffer_size);
			memset(replyBuf, 0, sizeof(replyBuf));

			// 接收客户端发来的命令
			recvLen = recv(acceptSockfd, recvBuf, recv_buffer_size-1, 0);
			if (recvLen <= 0) {
				close(acceptSockfd);
				exit(0);
			}

			// 解析并执行命令
			recvBuf[recvLen] = '\0';
			//printf("✅ 收到客户端命令: %s\n", recvBuf);
			printf("✅ 收到客户端请求，开始执行任务\n");
			ctl_command_process(recvBuf,acceptSockfd);
			// ===================== 关键：发送结束标记 =====================
			
			ReplyToClient(acceptSockfd, "END\n");

			// ===================== 关闭客户端连接，子进程退出 =====================
			close(acceptSockfd);
			exit(0);
		}
		close(acceptSockfd);
	} else {
		handle_error("accept");
	}
	}

	close(serverSockfd);
	serverSockfd = -1;
}

int SendMessage_Center(char *message, char recvBuf[], int len) {
    if (DataR_Mode == 0) {
        int center_socket;
        struct sockaddr_in server_addr;

        bzero(&server_addr, sizeof(struct sockaddr_in));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = inet_addr(control_center_ip);
        server_addr.sin_port = htons(control_center_port);

        if ((center_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            perror("socket 错误!\n");
            return -2;
        }

        if (connect(center_socket, (struct sockaddr*)&server_addr, sizeof(struct sockaddr)) < 0) {
            perror("连接控制中心失败，请确认控制中心是否启动!\n");
            close(center_socket); // 连接失败也要关闭
            return -1;
        }

        send(center_socket, message, strlen(message), 0);
        int recvLen = recv(center_socket, recvBuf, len-1, 0);

        // ================= 无论成功失败，都先关闭 socket =================
        close(center_socket);

        if (recvLen <= 0) {
            return 1;
        } else {
            recvBuf[recvLen] = '\0';
            return 0;
        }
    } else {
        printf("%s\n", message);
    }
    return 0;
}
