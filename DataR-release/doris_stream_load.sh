#!/bin/bash
# ===================================================================
# doris_stream_load.sh — 通过 curl HTTP Stream Load 将 JSON 文件导入 Doris
#
# 用法: bash doris_stream_load.sh <filepath> <host> <port> <db> <table> <user> <password>
#
# 参数:
#   $1 - filepath    JSON 数据文件路径
#   $2 - host        Doris 服务器地址
#   $3 - port        Doris HTTP 端口
#   $4 - db          Doris 数据库名
#   $5 - table       Doris 表名
#   $6 - user        Doris 用户名
#   $7 - password    Doris 密码
# ===================================================================

set -e

FILEPATH="$1"
HOST="$2"
PORT="$3"
DB="$4"
TABLE="$5"
USER="$6"
PASSWORD="$7"

if [ -z "$FILEPATH" ] || [ -z "$HOST" ] || [ -z "$PORT" ] || [ -z "$DB" ] || [ -z "$TABLE" ] || [ -z "$USER" ] || [ -z "$PASSWORD" ]; then
    echo "ERROR: Missing arguments. Usage: $0 <filepath> <host> <port> <db> <table> <user> <password>" >&2
    exit 1
fi

if [ ! -f "$FILEPATH" ]; then
    echo "ERROR: File not found: $FILEPATH" >&2
    exit 1
fi

LABEL="load_$(uuidgen)"

curl --location-trusted \
    -u "${USER}:${PASSWORD}" \
    -H "format: json" \
    -H "strip_outer_array: false" \
    -H "key_name_case_sensitive: false" \
    -H "read_json_by_line: true" \
    -H "label: ${LABEL}" \
    -T "${FILEPATH}" \
    "http://${HOST}:${PORT}/api/${DB}/${TABLE}/_stream_load" 2>/dev/null

exit $?