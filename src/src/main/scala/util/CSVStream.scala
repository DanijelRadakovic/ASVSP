package util

import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object CSVStream {

  val schema: StructType = new StructType()
    .add("ip", StringType)
    .add("date", StringType)
    .add("time", StringType)
    .add("zone", StringType)
    .add("cik", StringType)
    .add("accession", StringType)
    .add("extension", StringType)
    .add("code", FloatType)
    .add("size", DoubleType)
    .add("idx", FloatType)
    .add("noRefer", FloatType)
    .add("noAgent", FloatType)
    .add("find", FloatType)
    .add("crawler", FloatType)
    .add("browser", StringType)

  def read(session: SparkSession, directory: String): DataFrame = {
    session.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(directory)
  }

  def write(data: Dataset[_], directory: String, merge: Boolean): Unit = {
    if (merge) {
      data.coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(directory)
    } else {
      data.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(directory)
    }
  }
}
