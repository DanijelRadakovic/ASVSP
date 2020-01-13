#!/bin/bash
cd map_reduce

printf "\nSET CLASSPATHS\n"

export CLASSPATH="$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-core-$HADOOP_VERSION.jar:$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-common-$HADOOP_VERSION.jar:$HADOOP_PREFIX/share/hadoop/common/hadoop-common-$HADOOP_VERSION.jar:~map_reduce/*:$HADOOP_PREFIX/lib/*"

printf "\nCOMPILE JAVA\n"

javac -d . ProcessWebStatus.java && printf "\nBUILD JAR\n" && jar cfm ProcessWebStatus.jar Manifest.txt *.class && printf "\nCLEAN UP\n" && rm -f *.class

printf "\nRUN MAP-REDUCE\n"

hadoop jar ProcessWebStatus.jar ProcessWebStatus /data /report/web_status && printf "\nRESULTS\n" && hdfs dfs -cat /report/web_status/*

