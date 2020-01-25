# ASVSP

Download data [here](https://drive.google.com/open?id=1LLD3UH2mRa3Oz7omJo8J8I1epukpDFLz). Copy downloaded **data** directory into **docker** directory


## Environment setup
- `$ cd docker`
- `$ docker-compose up --build`

## Data initialization
`$ ./scripts/init_data.sh`

## Running batch processing
`./scripts/run_spark_batch.sh apache-batch-log-analyzer.jar command [-m] input-dir output-dir`

- command = status|ip|general|doc|datetime|not-found
- m = merge results to one csv file
- inpug-dir = path to directory where data are stored
- output-dir = path to directory where results will be stored

`$ ./scripts/run_spark_batch.sh ./bin/apache-batch-log-analyzer.jar status -m /data /report/status`

## Running stream processing

`$ ./scripts/run_spark_stream.sh ./bin/apache-stream-log-analyzer_2.11-0.1.jar zookeeper topic [-p output-dir]`

- zookeeper = newtwork:port to zookeeper
- topic = name of topic
- -p = persist upocoming logs and processing results
- output-dir = path to direcory where upcoming logs will be stored 

Run stream processing without saving data:

`$ ./scripts/run_spark_stream.sh ./bin/apache-stream-log-analyzer_2.11-0.1.jar zoo:2181 apache-access-log`

Run stream processing and save data:

`$ ./scripts/run_spark_stream.sh ./bin/apache-stream-log-analyzer_2.11-0.1.jar zoo:2181 apache-access-log -p /logs`
