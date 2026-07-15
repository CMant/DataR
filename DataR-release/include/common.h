#ifndef _COMMON_H_
#define _COMMON_H_
#include <glib.h>
#include <ctype.h>
extern GHashTable *g_task_config;
// typedef struct TASK_INF{
// 	gchar *task_name;
// 	gchar *task_speed;
// 	gchar *migrate_type;
// 	gchar *pump_sql;
// 	gchar *parallel_thread_per_task;
// 	gchar *send_buffer_size;
// 	gchar *source_host;
// 	gchar *source_port;
// 	gchar *source_dbname;
// 	gchar *source_password;
// 	gchar *source_username;
	
// 	gchar *dest_username;
// 	gchar *dest_password;
// 	gchar *dest_host;
// 	gchar *dest_port;
// 	gchar *dest_dbname;
// 	gchar *dest_table;



// } TASK_INF;

typedef struct {
    const char *cfg_key;
    size_t offset;
} TaskFieldMap;
char *left_trim(char *str);
char *right_trim(char *str);
char *trim(char *str);
char * Get_Conf_Value(const char *kev);
void FastStrcat(char **pszDest, const char* pszSrc,int *len);
void FastStrcat2(char **pszDest, const char* pszSrc);

static inline const char* get_cfg(const char *key)
{
    if (!g_task_config)
        return NULL;
    return g_hash_table_lookup(g_task_config, key);
}






void free_global_task_cfg(void);


// void fill_task_info(TASK_INF *info);

// // 释放结构体
// void free_task_info(TASK_INF *info);



 

void hash_val_free(gpointer data);

int parse_config_text(const char *cfg_data);

#endif