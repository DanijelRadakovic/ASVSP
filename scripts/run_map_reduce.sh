#!/bin/bash
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /report/web_status"
docker exec -it namenode bash -c "chmod +x /map_reduce/web_status.sh && /map_reduce/web_status.sh"
