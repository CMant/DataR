# ================================
# 基础配置
# ================================
IDIR            := include
CC              := gcc
CFLAGS          := -Wall -Wextra -std=gnu99 -I$(IDIR) -fPIC
LDFLAGS         := -rdynamic -Wl,-rpath='$$ORIGIN/lib'

PKG_CFLAGS      := $(shell pkg-config --cflags glib-2.0 libpq )
PKG_LDLIBS      := $(shell pkg-config --libs   glib-2.0 libpq )

# 新增 MySQL 头文件 + 库路径
MYSQL_INC       := -I/usr/include/mysql
MYSQL_LIB       := -L/usr/lib64/mysql -lmysqlclient

CFLAGS          += $(PKG_CFLAGS) $(MYSQL_INC)
LDLIBS          := -ldl -lpthread -lrt

# 主程序
TARGET          := DataR
OBJ             := common.o \
                   ctl_command.o \
                   adj_speed.o \
                   read_from_db.o \
                   network_service.o \
                   DataR.o \
                   init_check.o \
                   adj_speed.o

# 动态库
LIBPG           := libpg_to_pg.so
LIBPG_TO_MYSQL  := libpg_to_mysql.so
LIBPG_OBJ       := pg_to_pg.o
LIBPG_MYSQL_OBJ := pg_to_mysql.o


LIBPG_TO_FILE   := libpg_to_file.so
LIBPG_FILE_OBJ  := pg_to_file.o




DEPS            := $(patsubst %.o,%.d,$(OBJ)) $(patsubst %.o,%.d,$(LIBPG_OBJ)) $(patsubst %.o,%.d,$(LIBPG_MYSQL_OBJ)) $(patsubst %.o,%.d,$(LIBPG_FILE_OBJ))


# ================================
# 编译全部
# ================================
all: $(TARGET) $(LIBPG) $(LIBPG_TO_MYSQL) $(LIBPG_TO_FILE) copy_libs

# 主程序
$(TARGET): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS) $(PKG_LDLIBS) $(LDLIBS) $(MYSQL_LIB)

# 动态库 libpg_to_pg.so
$(LIBPG): $(LIBPG_OBJ)
	$(CC) -shared -o $@ $^
	mkdir -p lib
	mv $(LIBPG) lib/

# 动态库 libpg_to_mysql.so（完整合并你的编译参数）
$(LIBPG_TO_MYSQL): $(LIBPG_MYSQL_OBJ)
	$(CC) -shared -o $@ $^ $(MYSQL_LIB) $(PKG_LDLIBS) -pthread
	 
	mv $(LIBPG_TO_MYSQL) lib/

# 动态库 libpg_to_file.so
$(LIBPG_TO_FILE): $(LIBPG_FILE_OBJ)
	$(CC) -shared -o $@ $^
	 
	mv $(LIBPG_TO_FILE) lib/

# ================================
# 自动拷贝所有依赖库到 lib（核心功能）
# ================================
copy_libs: $(TARGET)
	mkdir -p lib
	ldd $(TARGET) | grep "=>" | awk '{print $$3}' | grep -v "libc.so.6" | xargs -I {} cp -n {} lib/ 2>/dev/null || true
	@echo "✅ 所有依赖库已自动复制到 ./lib 目录（已过滤 libc.so.6）"

# ================================
# 通用规则
# ================================
%.o: %.c %.d
	$(CC) -c $< -o $@ $(CFLAGS)

%.d: %.c
	$(CC) -MM $< $(CFLAGS) > $@

-include $(DEPS)

# ================================
# 清理
# ================================
.PHONY: clean
clean:
	rm -f $(OBJ) $(DEPS) $(TARGET)
	rm -rf lib *.so