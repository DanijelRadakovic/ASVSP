package util

import model.WebReport
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.LongType

object Summary {


  def summary(data: Dataset[WebReport]): Dataset[WebReport] = {
    val session =SparkSession.builder().getOrCreate()
    import session.implicits._

    data.unionByName(data.groupBy().min()
      .withColumn("data", lit("Min"))
      .withColumn("min(averageTA)", lit(0))
      .withColumnRenamed("min(hits)", "hits")
      .withColumnRenamed("min(txAmount)", "txAmount")
      .withColumnRenamed("min(averageTA)", "averageTA")
      .withColumnRenamed("min(visitors)", "visitors")
      .withColumn("hits", $"hits".cast(LongType))
      .withColumn("txAmount", $"txAmount".cast(LongType))
      .withColumn("visitors", $"visitors".cast(LongType))
      .as[WebReport]
    ).unionByName(data.groupBy().max()
      .withColumn("data", lit("Max"))
      .withColumn("max(averageTA)", lit(0))
      .withColumnRenamed("max(hits)", "hits")
      .withColumnRenamed("max(txAmount)", "txAmount")
      .withColumnRenamed("max(averageTA)", "averageTA")
      .withColumnRenamed("max(visitors)", "visitors")
      .withColumn("hits", $"hits".cast(LongType))
      .withColumn("txAmount", $"txAmount".cast(LongType))
      .withColumn("visitors", $"visitors".cast(LongType))
      .as[WebReport]
    ).unionByName(data.groupBy().sum()
      .withColumn("data", lit("Sum"))
      .withColumn("sum(averageTA)", lit(0))
      .withColumnRenamed("sum(hits)", "hits")
      .withColumnRenamed("sum(txAmount)", "txAmount")
      .withColumnRenamed("sum(averageTA)", "averageTA")
      .withColumnRenamed("sum(visitors)", "visitors")
      .withColumn("hits", $"hits".cast(LongType))
      .withColumn("txAmount", $"txAmount".cast(LongType))
      .withColumn("visitors", $"visitors".cast(LongType))
      .as[WebReport]
    )
  }
}
