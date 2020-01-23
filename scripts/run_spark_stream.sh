EXEC=$(basename "${1}")
docker cp "${1}" spark-master:/
docker exec spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ${EXEC} ${2} ${3} ${4}"
# Example
# run_spark_stream apache-stream-log-analyzer_2.11-01.jar zoo:2181 /logs hello
