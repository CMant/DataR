#ifndef _COMMON_H_
#define _COMMON_H_
#include <glib.h>
#include <ctype.h>


char *left_trim(char *str);
char *right_trim(char *str);
char *trim(char *str);
char * Get_Conf_Value(char *kev);
int FastStrcat(char **pszDest, const char* pszSrc,int *len);
typedef struct TASK_INF{
	gchar *task_name;
	gchar *migrate_type;
	gchar *source_host;
	gchar *source_port;
	gchar *source_dbname;
	gchar *source_username;
	gchar *source_password;
	gchar *dest_host;
	gchar *dest_port;
	gchar *dest_dbname;
	gchar *dest_username;
	gchar *dest_password;
	gchar *dest_table;
	gchar *task_speed;
	// gchar *deceleration_rounds;
	gchar *pump_sql;
	gchar *row_size;
	gchar *parallel_thread_per_task;
	gchar *send_buffer_size;
	gchar *max_tuple_size;
	}TASK_INF;


#endif