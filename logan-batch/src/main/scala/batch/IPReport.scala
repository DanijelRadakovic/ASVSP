package batch

import model.{WebLog, WebReport}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.Summary

class IPReport extends ProccesData {

  override def processData(data: Dataset[WebLog]): (Dataset[WebReport], DataFrame) = {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val ipReport = data.groupBy("ip").agg(
      count("code").alias("hits"),
      sum("size").alias("txAmount"),
      round(avg("size"), 2).alias("averageTA"),
      countDistinct("ip", "datetime", "crawler").alias("visitors")
    ).withColumnRenamed("ip", "data")
      .as[WebReport]
      .sort($"hits")
      .cache()

    val ipReportSummary = Summary.summary(ipReport)

    (ipReportSummary, ipReport.select("data", "hits", "visitors"))
  }
}
