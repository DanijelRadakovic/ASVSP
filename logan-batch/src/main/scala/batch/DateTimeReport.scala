package batch
import model.{WebLog, WebReport}
import org.apache.spark.sql.functions.{avg, count, countDistinct, round, sum}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.Summary

class DateTimeReport extends ProccesData {

  override def processData(data: Dataset[WebLog]): (Dataset[WebReport], DataFrame) = {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val dateTimeReport = data.groupBy("datetime").agg(
      count("code").alias("hits"),
      sum("size").alias("txAmount"),
      round(avg("size"), 2).alias("averageTA"),
      countDistinct("ip", "datetime", "crawler").alias("visitors")
    ).withColumn("data", $"datetime".cast(StringType))
      .drop("datetime")
      .as[WebReport]
      .sort($"hits")
      .cache()

    val dateTimeReportSummary = Summary.summary(dateTimeReport)

    (dateTimeReportSummary, dateTimeReport.select("data", "hits", "visitors"))
  }
}
