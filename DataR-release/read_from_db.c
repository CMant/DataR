// 必须放在 最最最第一行！
#define _GNU_SOURCE

#include  <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dlfcn.h>


#include "include/common.h"

#include <dlfcn.h>  // 必须加

// 函数指针类型（你原来应该有）
typedef int (*MigrateFunc)( int);

int Read_from_DB( int acceptSockfd)
{
    const char *tmp;

    tmp = get_cfg("migrate_type");
   

    //char *func_name = task_info.migrate_type;
    char *func_name= tmp ? g_strdup(tmp) : NULL;
    if (!func_name || strlen(func_name) == 0) {
        fprintf(stderr,"错误：迁移类型不能为空\n");
        return -1;
    }

    printf("您正在使用 %s 核心函数进行迁移\n", func_name);

    // ===================== 智能拼接动态库路径 =====================
    char lib_path[256] = {0};
    // 格式：./lib/func_name.so
    snprintf(lib_path, sizeof(lib_path), "./lib/lib%s.so", func_name);

    // ===================== 自动打开对应动态库 =====================
    void *lib_handle = dlopen(lib_path, RTLD_NOW | RTLD_GLOBAL);
    if (!lib_handle) {
        fprintf(stderr,"错误：无法加载动态库 [%s] → %s\n", lib_path, dlerror());
        return -1;
    }

    // ===================== 自动查找同名函数 =====================
    MigrateFunc func = (MigrateFunc)dlsym(lib_handle, func_name);
    if (!func) {
        fprintf(stderr,"错误：动态库 [%s] 中不存在函数 [%s()]\n", lib_path, func_name);
        dlclose(lib_handle);
        return -1;
    }

    // ===================== 执行函数 =====================
    printf("正在执行函数：%s()\n", func_name);
    int ret = func(acceptSockfd);
 
    // 关闭库
    dlclose(lib_handle);
    return ret;
}