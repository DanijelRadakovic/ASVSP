package stream

import model.ApacheAccessLog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.round
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class IPAddressWatcher extends Watcher {
  override val name: String = "ip-watcher"

  override def watch(logs: DStream[ApacheAccessLog]): Unit = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    logs.flatMap(log => {
      val tokens = log.request.split(" ")
      if (tokens.length == 3) Some(((tokens(1), log.ip, log.agent), (1L, log.bytes)))
      else None
    }).reduceByKeyAndWindow(
      (x: (Long, Long), y: (Long, Long)) => (x._1 + y._1, x._2 + y._2),
      (x: (Long, Long), y: (Long, Long)) => (x._1 - y._1, x._2 - y._2),
      Seconds(300), Seconds(10))
      .map(x => (x._1._1, (x._2._1, 1L, x._2._2)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, x._2._1, x._2._2, x._2._3))
      .transform(rdd => rdd.sortBy(x => x._2, ascending = false)) // sort by hits
      .foreachRDD(rdd => if (!rdd.isEmpty()) rdd.toDF("URL", "Hits", "Visitors", "TX Amount")
        .withColumn("Average TA", round($"TX Amount" / $"Hits", 2))
        .show(15, truncate = false))
  }
}
