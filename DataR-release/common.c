#include <glib.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define APP_NAME "DATAR"
#define CONFIG_FILE "R.ini"

// ==============================
// 全局只加载一次配置（高性能）
// ==============================
static GKeyFile *g_config = NULL;

// 初始化配置（程序启动时调用一次）
static void config_init_once(void)
{
    if (g_config != NULL)
        return;

    g_config = g_key_file_new();
    if (!g_key_file_load_from_file(g_config, CONFIG_FILE, G_KEY_FILE_NONE, NULL)) {
        fprintf(stderr,"警告：配置文件 %s 加载失败\n", CONFIG_FILE);
    }
}

// ==============================
// 安全获取配置（自动去空格、自动释放）
// ==============================
char *Get_Conf_Value(const char *key)
{
    if (key == NULL)
        return NULL;

    // 只初始化一次
    config_init_once();

    g_autofree gchar *glib_value = g_key_file_get_string(g_config, APP_NAME, key, NULL);
    if (glib_value == NULL)
        return NULL;

    // 去空格并返回拷贝（安全、无泄漏）
    char *trimmed = g_strstrip(g_strdup(glib_value));
    return trimmed;
}

// ==============================
// 安全 trim 系列（空指针安全）
// ==============================
char *left_trim(char *str)
{
    if (str == NULL || *str == '\0')
        return str;

    char *begin = str;
    while (isspace((unsigned char)*begin))
        begin++;

    memmove(str, begin, strlen(begin) + 1);
    return str;
}

char *right_trim(char *str)
{
    if (str == NULL || *str == '\0')
        return str;

    size_t len = strlen(str);
    char *end = str + len - 1;
    while (end >= str && isspace((unsigned char)*end))
        end--;
    *(end + 1) = '\0';
    return str;
}

char *trim(char *str)
{
    if (str == NULL)
        return NULL;
    return right_trim(left_trim(str));
}

void FastStrcat(char **Dest, const char* Src, int *len)
{
    // 空指针防御（不影响性能，防止崩溃）
    if (!Dest || !*Dest || !Src || !len)
        return;

    // 高速拼接，不补 \0，支持多次连续拼接
    while (*Src) {
        **Dest = *Src;
        (*Dest)++;
        Src++;
        (*len)++;
    }
}

void FastStrcat2(char **Dest, const char* Src)
{
    // 空指针防御（不影响性能，防止崩溃）
    if (!Dest || !*Dest || !Src)
        return;

    // 高速拼接，不补 \0，支持多次连续拼接
    while (*Src) {
        **Dest = *Src;
        (*Dest)++;
        Src++;
    
    }
}