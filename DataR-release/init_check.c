#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <glib.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#define APP_NAME "DATAR"
#define CONFIG_FILE "R.ini"

/*************** 文件全局变量 *******************/
char *sync_db_type = NULL;
int row_size = 0;
int listen_backlog = 0;
long long max_tuple_size = 0;
int deceleration_rounds = 0;
int calculate_speed_rounds = 0;
char *control_center_ip = NULL;
long control_center_port = 0;
char *DataR_listener_ip = NULL;
int DataR_listener_port = 0;
char *auth_key = NULL;
char *kafka_bootstrap_servers = NULL;
int parallel_thread_per_task = 0;
int recv_buffer_size=81920;
int send_buffer_size = 0;
char *DataR_node_name = NULL;
int DataR_Mode = 1; // 默认单机模式，更安全

extern int SendMessage_Center(char *message, char recvBuf[], int len);

/*************** 函数体 ********************/

// 检查配置文件是否存在、可读
int config_file_check(char *config_filename) {
    if (access(config_filename, F_OK) == -1) {
        perror("配置文件不存在");
        exit(-1);
    }
    if (access(config_filename, R_OK) == -1) {
        perror("配置文件无读取权限");
        exit(-1);
    }
    printf("配置文件 %s 检查通过\n", config_filename);
    return 0;
}

// 安全获取字符串 + 自动释放glib内存
static char* safe_get_string(GKeyFile *config, const char *group, const char *key) {
    GError *err = NULL;
    char *val = g_key_file_get_string(config, group, key, &err);

    if (err) {
        g_error_free(err);
        return NULL;
    }

    if (!val || strlen(val) == 0) {
        g_free(val);
        return NULL;
    }

    return val;
}

// 安全转 int
static int safe_atoi(const char *s) {
    if (!s) return 0;
    return atoi(s);
}

// 安全转 long
static long safe_atol(const char *s) {
    if (!s) return 0;
    return atol(s);
}

// 配置文件赋值
void config_value_init(char *config_filename) {
    GError *err = NULL;
    GKeyFile *config = g_key_file_new();

    if (!g_key_file_load_from_file(config, config_filename, G_KEY_FILE_NONE, &err)) {
        fprintf(stderr, "加载配置失败：%s\n", err->message);
        g_error_free(err);
        g_key_file_free(config);
        exit(-1);
    }

    char *tmp;

    // listen_backlog
    tmp = safe_get_string(config, APP_NAME, "listen_backlog");
    listen_backlog = safe_atoi(tmp);
    g_free(tmp);
    if (listen_backlog <= 0) {
        fprintf(stderr, "listen_backlog 配置错误\n");
        exit(-1);
    }

    // control_center
    control_center_ip = safe_get_string(config, APP_NAME, "control_center_ip");
    tmp = safe_get_string(config, APP_NAME, "control_center_port");
    control_center_port = safe_atol(tmp);
    g_free(tmp);

    //recv_buffer_size
    
    tmp = safe_get_string(config, APP_NAME, "recv_buffer_size");
    recv_buffer_size = safe_atoi(tmp);
    g_free(tmp);

    // DataR_listener_ip
    DataR_listener_ip = safe_get_string(config, APP_NAME, "DataR_listener_ip");
    if (!DataR_listener_ip) {
        fprintf(stderr, "DataR_listener_ip 未配置\n");
        exit(-1);
    }

    // DataR_listener_port
    tmp = safe_get_string(config, APP_NAME, "DataR_listener_port");
    DataR_listener_port = safe_atoi(tmp);
    g_free(tmp);
    if (DataR_listener_port <= 0) {
        fprintf(stderr, "DataR_listener_port 配置错误\n");
        exit(-1);
    }

    // deceleration_rounds
    tmp = safe_get_string(config, APP_NAME, "deceleration_rounds");
    deceleration_rounds = safe_atoi(tmp);
    g_free(tmp);

    // calculate_speed_rounds
    tmp = safe_get_string(config, APP_NAME, "calculate_speed_rounds");
    calculate_speed_rounds = safe_atoi(tmp);
    g_free(tmp);

    auth_key = safe_get_string(config, APP_NAME, "auth_key");
    DataR_node_name = safe_get_string(config, APP_NAME, "DataR_node_name");

    if (!DataR_node_name) {
        fprintf(stderr, "DataR_node_name 未配置\n");
        exit(-1);
    }

    printf("======== 配置加载完成 ========\n");
    printf("control_center_ip           :%s\n", control_center_ip ? control_center_ip : "(空)");
    printf("control_center_port         :%ld\n", control_center_port);
    printf("DataR_listener_ip           :%s\n", DataR_listener_ip);
    printf("DataR_listener_port         :%d\n", DataR_listener_port);
    printf("auth_key                    :%s\n", auth_key ? auth_key : "(空)");
    printf("listen_backlog              :%d\n", listen_backlog);
    printf("deceleration_rounds         :%d\n", deceleration_rounds);
    printf("calculate_speed_rounds      :%d\n", calculate_speed_rounds);
    printf("recv_buffer_size            :%d\n", recv_buffer_size);
    printf("DataR_node_name             :%s\n", DataR_node_name);

    g_key_file_free(config);
}

// 初始化调度中心网络连接
int init_Send_to_Center(int *client_sockfd) {
    *client_sockfd = -1;

    if (!control_center_ip || strlen(control_center_ip) == 0 || control_center_port <= 0) {
        printf("未配置控制中心，跳过连接\n");
        return 0;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket 创建失败");
        return -1;
    }

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(control_center_port);

    if (inet_pton(AF_INET, control_center_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "control_center_ip 非法：%s\n", control_center_ip);
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("连接控制中心失败");
        close(sock);
        return -1;
    }

    char LOGIN_MESSAGE[1024] = {0};
    char recv_center_signal[1024] = {0};
    snprintf(LOGIN_MESSAGE, sizeof(LOGIN_MESSAGE), "LOGIN IN %s", DataR_node_name);

    if (SendMessage_Center(LOGIN_MESSAGE, recv_center_signal, 1024) < 0) {
        fprintf(stderr, "发送登录失败\n");
        close(sock);
        return -1;
    }

    printf("控制中心返回：%s\n", recv_center_signal);
    if (strstr(recv_center_signal, "LOGIN OK")) {
        printf("登录控制中心成功\n");
        *client_sockfd = sock;
        return 0;
    } else {
        fprintf(stderr, "登录失败！\n");
        close(sock);
        return -1;
    }
}

// 模式判断
int check_center_connect() {
    if (!control_center_ip || strlen(control_center_ip) == 0 || control_center_port == 0) {
        printf("=> 进入 Standalone 模式\n");
        DataR_Mode = 1;
    } else {
        printf("=> 进入 Center 模式\n");
        DataR_Mode = 0;
    }
    return DataR_Mode;
}

int init_check() {
    config_file_check(CONFIG_FILE);
    config_value_init(CONFIG_FILE);
    check_center_connect();
    return 0;
}