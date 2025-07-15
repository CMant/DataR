#include  <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "include/pg_to_pg.h"
#include "include/pg_to_mysql.h"
#include "include/common.h"
int Read_from_DB(TASK_INF task_info,char * result_num){
	if(0 == memcmp(task_info.migrate_type, "pg_to_pg", strlen("pg_to_pg"))){
		printf("%s\n","pg_to_pg");
		pg_to_pg(task_info,result_num);

	}else if (0 == memcmp(task_info.migrate_type, "pg_to_mysql", strlen("pg_to_mysql"))){
		pg_to_mysql(task_info,result_num);

	}else{

		perror("sync_db_type 类型不存在");
		exit(-1);
	}

	return 0;
}