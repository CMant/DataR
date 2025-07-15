
#include <stdio.h>
#include<sys/shm.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include<fcntl.h>
#include <sys/types.h>
#include "include/message_type.h"
//共享内存模式设置sleeptime
extern int send_to_center_sockfd;
int Set_sleepValue(int sleep_value,char *taskname){
    int fd=shm_open(taskname,O_CREAT|O_RDWR,0666);
    if(fd!=0){
        
        ftruncate(fd,16);
        char *p=mmap(NULL,16,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
       
        sprintf(p,"%d",sleep_value);
        
        close(fd);
        munmap(p,16);
        return 0;
    }else{
 
        return 1;
    }

}
int Get_sleepValue(char *taskname){
    
    int sleep_time;
    int fd=shm_open(taskname,O_CREAT|O_RDWR,0666);
    ftruncate(fd,16);
    char *p=mmap(NULL,16,PROT_READ,MAP_SHARED,fd,0);
    sleep_time=atoi(p);
    close(fd);
    munmap(p,16);
    return sleep_time;
}