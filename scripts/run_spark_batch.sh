EXEC=$(basename "${1}")
docker cp "${1}" spark-master:/
docker exec spark-master bash -c "/spark/bin/spark-submit --master spark://spark-master:7077 ${EXEC} ${2} ${3} ${4} ${5}"

# Example
# run_spark_batch apache-log-analyzer_2.11-01.jar general -m /tmp /tmp_report/general