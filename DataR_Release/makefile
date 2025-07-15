IDIR =include
CC=gcc
CFLAGS= `pkg-config --cflags --libs glib-2.0 libpq mysqlclient`  -L. -Wl,-rpath='$$ORIGIN' -lpg_to_pg -lpg_to_mysql
TARGET = DataR
OBJ=   common.o ctl_command.o  speed_adj.o  read_from_db.o network_service.o DataR.o init_check.o  

$(TARGET): $(OBJ)
	$(CC) -o $(TARGET) $(OBJ) $(CFLAGS)


read_from_db.o:read_from_db.c
	$(CC) -c  read_from_db.c  `pkg-config --cflags --libs  glib-2.0 libpq`

ctl_command.o:ctl_command.c
	$(CC) -c  ctl_command.c `pkg-config --cflags --libs  glib-2.0 libpq`
speed_adj.o:speed_adj.c
	$(CC) -c  speed_adj.c
DataR.o:DataR.c
	$(CC) -c  DataR.c
common.o:common.c
	$(CC) -c  common.c `pkg-config --cflags --libs glib-2.0`
# pg_to_pg.o:pg_to_pg.c
# 	$(CC) -c  pg_to_pg.c `pkg-config --cflags --libs glib-2.0 libpq`
# pg_to_mysql.o:pg_to_mysql.c
# 	$(CC) -c  pg_to_mysql.c  `pkg-config --cflags --libs glib-2.0 mysqlclient libpq`  -lmysqlclient 

network_service.o:network_service.c
	$(CC) -c  network_service.c `pkg-config --cflags --libs glib-2.0`
init_check.o:init_check.c
	$(CC) -c  init_check.c `pkg-config --cflags --libs glib-2.0`

.PHONY: clean
clean:
	rm -f *.o $(TARGET)
