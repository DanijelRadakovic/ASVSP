import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import stream.WatcherFactory
import util.ApacheLogParser._

object LogStreamAnalyzer extends App {

  val logger = Logger.getRootLogger
  logger.setLevel(Level.ERROR)

  var zoo = ""
  var topic = ""
  var path = ""

  if (args.length < 3) {
    logger.error("Required at least 3 arguments: zoo:port path topic [topic ... topic]")
  } else {
    zoo = args(0)
    topic = args(1)
    path = args(2)
  }

  val ssc = new StreamingContext(new SparkConf().setAppName("LogStreamAnalyzer"), Seconds(5))
  ssc.checkpoint("/checkpoint")
  val sqlContext = SparkSession.builder().getOrCreate()

  import sqlContext.implicits._


  val kcs = KafkaUtils.createStream(ssc, zoo, "web-log-consumer", Map(topic -> 1))
  val rawLogs = kcs.map(x => x._2).cache()

  val logs = rawLogs.flatMap(parseStringLog).cache()

  WatcherFactory.getAllWatchers.foreach(watcher => watcher.watch(logs))

//  logs.foreachRDD(rdd => rdd.toDS()
//    .coalesce(1)
//    .write
//    .mode(SaveMode.Append)
//    .text(path))


  ssc.start()
  ssc.awaitTermination()
}