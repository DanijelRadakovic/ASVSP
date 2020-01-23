package watch
import model.{ApacheAccessLog, BlacklistAlarm}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

class BruteForceAttack extends Watcher with Serializable {
  override val path: String = "/ip-blacklist"

  override def watch(logs: DStream[ApacheAccessLog], persist: Boolean): Unit = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    logs.filter(log => log.status.startsWith("4") || log.status.startsWith("5"))
      .map(log => (log.ip, 1L))
      .reduceByKeyAndWindow(_ + _, _ + _, Minutes(10), Seconds(10))
      .filter(x => x._2 > 30)
      .foreachRDD(rdd => if(!rdd.isEmpty()) {
        val ds = rdd.toDF("ip", "errors")
          .withColumn("message", lit("Brute force attack!"))
          .withColumn("timestamp", current_timestamp())
          .as[BlacklistAlarm]
        ds.show(false)
        if (persist) ds.coalesce(1)
          .write
          .option("headers", "true")
          .mode(SaveMode.Append)
          .csv(path)
      })
  }
}
