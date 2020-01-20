package batch

import model.{NoRefererReport, WebLog, WebReport}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class GeneralReport extends ProccesData {

  override def processData(data: Dataset[WebLog]): (Dataset[WebReport], DataFrame) = {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val groupedByCode = data.groupByKey(log => {
      if (200 <= log.code && log.code < 300) "2xx Success"
      else if (300 <= log.code && log.code < 400) "3xx Redirection"
      else if (log.code == 404) "404 Not Found"
      else if (400 <= log.code && log.code < 500 && log.code != 404) "4xx Client Errors"
      else "5xx Server Errors"
    })

    val generalReport = groupedByCode.agg(
      count("code").as[WebReport].name("hits"),
      sum("size").as[WebReport].name("txAmount"),
      countDistinct("ip", "datetime", "crawler").as[WebReport].name("visitors"),
      sum("noRefer").as[NoRefererReport].name("noRefer")
    ).withColumnRenamed("value", "data")
      .withColumn("data", $"data".cast(StringType))
      .sort($"hits")
      .cache()

    var finalReport = generalReport.groupBy()
      .sum()
      .withColumnRenamed("sum(noRefer)", "noReferer")
      .withColumnRenamed("sum(hits)", "hits")
      .withColumnRenamed("sum(txAmount)", "txAmount")
      .withColumnRenamed("sum(visitors)", "visitors")
      .withColumn("noReferer", $"hits" - $"noReferer")

    val notFoundCnt = generalReport.select("hits").where($"data" === "404 Not Found")

    val successCnt = generalReport.select("hits")
      .where($"data" === "2xx Success" || $"data" === "3xx Redirection")
      .groupBy()
      .sum()


    val failedCnt = generalReport.select("hits")
      .where($"data" === "404 Not Found" || $"data" === "4xx Client Errors" || $"data" === "5xx Server Errors")
      .groupBy()
      .sum()

    if (!notFoundCnt.isEmpty) finalReport = finalReport
      .withColumn("response404", lit(1) * notFoundCnt.first()(0))
    else finalReport = finalReport.withColumn("response404", lit(0))

    if (!successCnt.isEmpty) finalReport = finalReport
      .withColumn("successfulRequests", lit(1) * successCnt.first()(0))
    else finalReport = finalReport.withColumn("successfulRequests", lit(0))

    if (!failedCnt.isEmpty) finalReport = finalReport
      .withColumn("failedRequest", lit(1) * failedCnt.first()(0))
    else finalReport = finalReport.withColumn("failedRequest", lit(0))

    (session.createDataset(Seq[WebReport]()), finalReport)
  }
}
