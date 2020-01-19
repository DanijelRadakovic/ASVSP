docker exec spark-master bash -c "/spark/bin/spark-submit --master spark://spark-master:7077 apache-log-analyzer_2.11-0.1.jar ${1} ${2} ${3} ${4}"
