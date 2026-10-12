#!/bin/bash
set -e
case "$1" in
  namenode)
    # format only on first start (empty volume)
    if [ ! -d /hadoop/dfs/name/current ]; then
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode ;;
  datanode)        exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager)     exec yarn nodemanager ;;
  *) exec "$@" ;;
esac
