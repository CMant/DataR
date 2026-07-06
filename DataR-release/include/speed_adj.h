#ifndef _SPEED_ADJ_H_
#define _SPEED_ADJ_H_
int Set_sleepValue(int sleep_value, char *taskname);
int Get_sleepValue(char *taskname);
int Del_sleep_tag(char *taskname);

// 新增：全局清理
void clean_all_shm(void);   
#endif

 