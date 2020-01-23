package watch

import model.ApacheAccessLog
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class StatusWatcher extends Watcher with Serializable {
  override val path: String = "/status-watcher"

  override def watch(logs: DStream[ApacheAccessLog], persist: Boolean): Unit = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    logs.map(log => {
      if (log.status.startsWith("2")) (("2xx Success", log.ip, log.agent), (1L, log.bytes))
      else if (log.status.startsWith("3")) (("3xx Redirection", log.ip, log.agent), (1L, log.bytes))
      else if (log.status.startsWith("4")) (("4xx Client error", log.ip, log.agent), (1L, log.bytes))
      else (("5xx Server error", log.ip, log.agent), (1L, log.bytes))
    }).reduceByKeyAndWindow(
      (x: (Long, Long), y: (Long, Long)) => (x._1 + y._1, x._2 + y._2),
      (x: (Long, Long), y: (Long, Long)) => (x._1 - y._1, x._2 - y._2),
      Seconds(300), Seconds(10))
      .map(x => (x._1._1, (x._2._1, 1L, x._2._2)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, x._2._1, x._2._2, x._2._3))
      .transform(rdd => rdd.sortBy(x => x._2, ascending = false)) // sort by hits
      .foreachRDD(rdd => if (!rdd.isEmpty()) {
        val ds = rdd.toDF("Status", "Hits", "Visitors", "TX Amount")
          .withColumn("Average TA", round($"TX Amount" / $"Hits", 2))
        ds.show()
        if (persist) ds.coalesce(1)
          .write
          .option("headers", "true")
          .mode(SaveMode.Append)
          .csv(path)
      })
  }
}
