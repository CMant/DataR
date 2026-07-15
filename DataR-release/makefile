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
 
KAFKA_LIB 		:= -L/usr/lib64 -lrdkafka

CFLAGS          += $(PKG_CFLAGS) $(MYSQL_INC)
LDLIBS          := -ldl -lpthread -lrt -lm

# 主程序
TARGET          := DataR
OBJ             := common.o \
                   ctl_command.o \
                   read_from_db.o \
                   network_service.o \
                   DataR.o \
                   init_check.o \
                   adj_speed.o \
				   cJSON.o \
				   pg_engine.o

# 动态库
LIBPG           := libpg_to_pg.so
LIBPG_TO_MYSQL  := libpg_to_mysql.so

LIBPG_OBJ       := pg_to_pg.o  pg_engine.o
LIBPG_MYSQL_OBJ := pg_to_mysql.o pg_engine.o

LIBPG_TO_FILE   := libpg_to_file.so
LIBPG_FILE_OBJ  := pg_to_file.o pg_engine.o

LIBPG_TO_KAFKA  := libpg_to_kafka.so
LIBPG_KAFKA_OBJ := pg_to_kafka.o

LIBPG_TO_JSON   := libpg_to_json.so
LIBPG_JSON_OBJ  := pg_to_json.o pg_engine.o

DEPS            := $(patsubst %.o,%.d,$(OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_MYSQL_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_FILE_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_KAFKA_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_JSON_OBJ))

# ================================
# 编译全部
# ================================
all: $(TARGET) $(LIBPG) $(LIBPG_TO_MYSQL) $(LIBPG_TO_FILE) $(LIBPG_TO_KAFKA) $(LIBPG_TO_JSON) copy_libs

# 主程序
$(TARGET): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS) $(PKG_LDLIBS) $(LDLIBS) $(MYSQL_LIB) $(KAFKA_LIB)

# 动态库 libpg_to_pg.so
$(LIBPG): $(LIBPG_OBJ)
	$(CC) -shared -o $@ $^
	mkdir -p lib
	mv $(LIBPG) lib/

# 动态库 libpg_to_mysql.so
$(LIBPG_TO_MYSQL): $(LIBPG_MYSQL_OBJ)
	$(CC) -shared -o $@ $^ $(MYSQL_LIB) $(PKG_LDLIBS) -pthread  
	mv $(LIBPG_TO_MYSQL) lib/

# 动态库 libpg_to_file.so
$(LIBPG_TO_FILE): $(LIBPG_FILE_OBJ)
	$(CC) -shared -o $@ $^ 
	mv $(LIBPG_TO_FILE) lib/

# 动态库 libpg_to_kafka.so
$(LIBPG_TO_KAFKA): $(LIBPG_KAFKA_OBJ) cJSON.o 
	$(CC) -shared -o $@ $^ -lm -pthread $(PKG_LDLIBS) $(KAFKA_LIB)
	mv $(LIBPG_TO_KAFKA) lib/

# 动态库 libpg_to_json.so
$(LIBPG_TO_JSON): $(LIBPG_JSON_OBJ) cJSON.o 
	$(CC) -shared -o $@ $^ -lm -pthread 
	mv $(LIBPG_TO_JSON) lib/

# ================================
# 自动拷贝所有依赖库到 lib
# ================================
copy_libs: $(TARGET)
	mkdir -p lib
	ldd $(TARGET) | grep "=>" | awk '{print $$3}' | grep -v "libc.so.6" | xargs -I {} cp -n {} lib/ 2>/dev/null || true
	cp -n /usr/lib64/librdkafka*.so* lib/ 2>/dev/null || true
	@echo "所有依赖库已自动复制到 ./lib 目录"

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
	rm -f $(LIBPG_OBJ) $(LIBPG_MYSQL_OBJ) $(LIBPG_FILE_OBJ) $(LIBPG_KAFKA_OBJ) $(LIBPG_JSON_OBJ)
	rm -rf lib/* *.so
