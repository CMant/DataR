#!/bin/bash
gcc -shared -fPIC -Iinclude  pg_to_file.c -o libpg_to_mysql.so  -pthread $(pkg-config --cflags --libs glib-2.0)
mv -f libpg_to_file.so lib/
rm -f /dev/shm/topic1
./DataR
