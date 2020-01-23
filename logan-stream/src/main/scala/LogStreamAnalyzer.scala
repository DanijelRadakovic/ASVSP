import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import watch.WatcherFactory
import util.ApacheLogParser._

object LogStreamAnalyzer extends App {

  val logger = Logger.getRootLogger
  logger.setLevel(Level.ERROR)

  var zoo = ""
  var topic = ""
  var path = ""
  var persist = false

  if (args.length != 2 && args.length != 4) {
    logger.error("Required at least 2 arguments: zoo:port topic [-p path]")
    sys.exit(1)
  } else if (args.length == 2) {
    zoo = args(0)
    topic = args(1)
  } else {
    zoo = args(0)
    topic = args(1)
    path = args(3)
    persist = true
  }

  val ssc = new StreamingContext(new SparkConf().setAppName("LogStreamAnalyzer"), Seconds(5))
  ssc.checkpoint("/checkpoint")
  val sqlContext = SparkSession.builder().getOrCreate()

  import sqlContext.implicits._

  val kcs = KafkaUtils.createStream(ssc, zoo, "web-log-consumer", Map(topic -> 1))
  val rawLogs = kcs.map(x => x._2).cache()

  val logs = rawLogs.flatMap(parseStringLog).cache()

  WatcherFactory.getAllWatchers.foreach(watcher => watcher.watch(logs, persist))

  if (persist) logs.foreachRDD(rdd => rdd.toDS().coalesce(1).write.mode(SaveMode.Append).csv(path))


  ssc.start()
  ssc.awaitTermination()
}