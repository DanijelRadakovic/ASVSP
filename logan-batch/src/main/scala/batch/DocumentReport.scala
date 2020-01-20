package batch
import model.{WebLog, WebReport}
import org.apache.spark.sql.functions.{avg, count, countDistinct, round, sum, concat, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.Summary

class DocumentReport extends ProccesData {

  override def processData(data: Dataset[WebLog]): (Dataset[WebReport], DataFrame) = {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val docReport = data.groupBy("cik","accession", "extension").agg(
      count("code").alias("hits"),
      sum("size").alias("txAmount"),
      round(avg("size"), 2).alias("averageTA"),
      countDistinct("ip", "datetime", "crawler").alias("visitors")
    ).withColumn("data", concat($"cik", lit("/"), $"accession", lit("/"), $"extension"))
      .drop("cik","accession", "extension")
      .as[WebReport]
      .sort($"hits")
      .cache()

    val docReportSummary = Summary.summary(docReport)

    (docReportSummary, docReport.select("data", "hits", "visitors"))
  }
}
