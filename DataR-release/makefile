# ================================
# 基础配置
# ================================
IDIR            := include
CC              := gcc
CFLAGS          := -Wall -Wextra -std=gnu99 -I$(IDIR) -fPIC
LDFLAGS         := -rdynamic -Wl,-rpath='$$ORIGIN/lib'

PKG_CFLAGS      := $(shell pkg-config --cflags glib-2.0 libpq )
PKG_LDLIBS      := $(shell pkg-config --libs   glib-2.0 libpq )

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
LIBPG_TO_FILE   := libpg_to_file.so
LIBPG_TO_KAFKA  := libpg_to_kafka.so
LIBPG_TO_JSON   := libpg_to_json.so
LIBPG_TO_DORIS  := libpg_to_doris.so

LIBPG_OBJ       := pg_to_pg.o  pg_engine.o
LIBPG_MYSQL_OBJ := pg_to_mysql.o pg_engine.o
LIBPG_FILE_OBJ  := pg_to_file.o pg_engine.o
LIBPG_KAFKA_OBJ := pg_to_kafka.o
LIBPG_JSON_OBJ  := pg_to_json.o pg_engine.o
LIBPG_DORIS_OBJ := pg_to_doris.o pg_engine.o

DEPS            := $(patsubst %.o,%.d,$(OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_MYSQL_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_FILE_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_KAFKA_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_JSON_OBJ)) \
                   $(patsubst %.o,%.d,$(LIBPG_DORIS_OBJ))

# ================================
# 编译全部
# ================================
.PHONY: all clean copy_libs
all: $(TARGET) $(LIBPG) $(LIBPG_TO_MYSQL) $(LIBPG_TO_FILE) $(LIBPG_TO_KAFKA) $(LIBPG_TO_JSON) $(LIBPG_TO_DORIS) copy_libs

# 主程序
$(TARGET): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS) $(PKG_LDLIBS) $(LDLIBS) $(MYSQL_LIB) $(KAFKA_LIB)

# libpg_to_pg.so
$(LIBPG): $(LIBPG_OBJ)
	$(CC) -shared -o $@ $^ $(PKG_LDLIBS)
	mkdir -p lib
	mv $(LIBPG) lib/

# libpg_to_mysql.so
$(LIBPG_TO_MYSQL): $(LIBPG_MYSQL_OBJ)
	$(CC) -shared -o $@ $^ $(MYSQL_LIB) $(PKG_LDLIBS) -pthread
	mv $(LIBPG_TO_MYSQL) lib/

# libpg_to_file.so
$(LIBPG_TO_FILE): $(LIBPG_FILE_OBJ)
	$(CC) -shared -o $@ $^ $(PKG_LDLIBS)
	mv $(LIBPG_TO_FILE) lib/

# libpg_to_kafka.so
$(LIBPG_TO_KAFKA): $(LIBPG_KAFKA_OBJ) cJSON.o
	$(CC) -shared -o $@ $^ -lm -pthread $(PKG_LDLIBS) $(KAFKA_LIB)
	mv $(LIBPG_TO_KAFKA) lib/

# libpg_to_json.so
$(LIBPG_TO_JSON): $(LIBPG_JSON_OBJ) cJSON.o
	$(CC) -shared -o $@ $^ -lm -pthread $(PKG_LDLIBS)
	mv $(LIBPG_TO_JSON) lib/

# libpg_to_doris.so
$(LIBPG_TO_DORIS): $(LIBPG_DORIS_OBJ) cJSON.o
	$(CC) -shared -o $@ $^ -lm -pthread $(PKG_LDLIBS)
	mv $(LIBPG_TO_DORIS) lib/

# ================================
# 拷贝依赖库
# ================================
copy_libs: $(TARGET)
	mkdir -p lib
	ldd $(TARGET) | grep '=>' | awk '{if($$3!="") print $$3}' | grep -v libc.so.6 | xargs -I {} cp -n {} lib/ 2>/dev/null || true
	cp -n /usr/lib64/librdkafka*.so* lib/ 2>/dev/null || true
	@echo "依赖库复制完成 ./lib"

# ================================
# 通用编译规则（关键修复！）
# ================================
%.o: %.c
	$(CC) -c $< -o $@ $(CFLAGS)

# 原子写入 .d 临时文件，根治无限循环
%.d: %.c
	$(CC) -MM -MT $*.o $(CFLAGS) $< > $@.tmp
	mv $@.tmp $@

-include $(DEPS)

# ================================
# 清理
# ================================
clean:
	rm -f *.o *.d *.d.tmp *.so $(TARGET)
	rm -rf lib