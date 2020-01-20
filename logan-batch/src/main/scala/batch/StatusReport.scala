package batch

import model.{WebLog, WebReport}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.Summary

class StatusReport extends ProccesData {

  override def processData(data: Dataset[WebLog]): (Dataset[WebReport], DataFrame) = {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val statusReport = data.groupByKey(log => {
      if (200 <= log.code && log.code < 300) "2xx Success"
      else if (300 <= log.code && log.code < 400) "3xx Redirection"
      else if (400 <= log.code && log.code < 500) "4xx Client Errors"
      else "5xx Server Errors"
    }).agg(
      count("code").as[WebReport].name("hits"),
      sum("size").as[WebReport].name("txAmount"),
      round(avg("size"), 2).as[WebReport].name("averageTA"),
      countDistinct("ip", "datetime", "crawler").as[WebReport].name("visitors")
    ).withColumnRenamed("value", "data")
      .withColumn("data", $"data".cast(StringType))
      .as[WebReport]
      .sort($"hits")
      .cache()

    val statusReportSummary = Summary.summary(statusReport)

    (statusReportSummary, statusReport.select("data", "hits", "visitors"))
  }
}
