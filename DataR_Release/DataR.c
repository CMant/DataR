/* ========================================================================== */
/*								   头文件					   				  */
/* ========================================================================== */
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <locale.h>
/* ===========speedadjust=============================================================== */
#include "include/speed_adj.h"
/* ===========network_service=============================================================== */
#include "include/network_service.h"
/* ===========ctl_command=============================================================== */
#include "include/ctl_command.h"
/* ===========初始化检查=============================================================== */
#include "include/init_check.h"
/* ========================================================================== */
/*								   宏定义									  */
/* ========================================================================== */
int main(int argc, char * argv[])
{
	setlocale(LC_ALL, "");
	pid_t pid;
	pid_t f_pid;
	int status;
	init_check();
	pid=fork();
	if(pid==0){
		ServerProcess();
	}
	else{
		f_pid = waitpid(-1,&status,0);
	}
	return 0;
}
