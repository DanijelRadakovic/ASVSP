package watch

import model.{ApacheAccessLog, DoSAlarm}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class DoSAttack extends Watcher with Serializable {

  override val path: String = "/dos-attack"

  override def watch(logs: DStream[ApacheAccessLog], persist: Boolean): Unit = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    logs.filter(log => log.status.startsWith("4") || log.status.startsWith("5"))
      .countByWindow(Seconds(60), Seconds(5))
      .filter(x => x > 50)
      .foreachRDD(rdd => if (!rdd.isEmpty()) {
        val ds = rdd.toDF("errors")
          .withColumn("message", lit("Dos Attack!"))
          .withColumn("timestamp", current_timestamp())
          .as[DoSAlarm]
        ds.show(false)
        if (persist) ds.coalesce(1)
          .write
          .option("headers", "true")
          .mode(SaveMode.Append)
          .csv(path)
      })
  }
}
