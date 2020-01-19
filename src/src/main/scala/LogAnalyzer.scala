import batch.ProcessDataResolver
import model.WebLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import util.CSVStream


object LogAnalyzer extends App {

  // Set the log level to only print errors
  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)

  var command = ""
  var sourcePath = ""
  var destPath = ""
  var merge = false

  if ((args.length != 3 && args.length != 4) || (args.length == 3 &&
    (args(0) == "-m" || args(1) == "-m" || args(2) == "-m"))) {
    logger.error("Required 3 arguments: command, (-m optional flag for merging results), " +
      "path to source directory, path to destination directory")
    sys.exit(1)
  } else if (args.length == 3) {
    command = args(0)
    sourcePath = args(1)
    destPath = args(2)
  } else {
    command = args(0)
    merge = true
    sourcePath = args(2)
    destPath = args(3)
  }

  val cmd = ProcessDataResolver.factory(command)
  if (cmd.isEmpty) {
    logger.error(s"Command: $command could not be found")
    sys.exit(1)
  }

  val spark = SparkSession.builder()
    .appName("LogAnalyzer")
    .getOrCreate()

  import spark.implicits._

  val rawData = CSVStream.read(spark, sourcePath)

  // transform raw data to more appropriate shape
  val data = rawData.withColumn("datetime", to_timestamp(concat($"date", lit(" "), $"time"),
    "yyyy-MM-dd HH:mm:ss")) // merge date and time to timestamp
    .drop(Seq("zone", "browser", "date", "time"): _*) // drop useless columns
    .na.drop() // drop rows with null values
    .withColumn("cik", $"cik".cast(IntegerType))
    .withColumn("code", $"code".cast(IntegerType))
    .withColumn("size", $"size".cast(IntegerType))
    .withColumn("idx", $"idx".cast(IntegerType))
    .withColumn("noRefer", $"noRefer".cast(IntegerType))
    .withColumn("noAgent", $"noAgent".cast(IntegerType))
    .withColumn("find", $"find".cast(IntegerType))
    .withColumn("crawler", $"crawler".cast(IntegerType))
    .as[WebLog]


  val reports = cmd.get.processData(data)

  CSVStream.write(reports._1, destPath + "/summary", merge)
  CSVStream.write(reports._2, destPath + "/report", merge)

  spark.stop()
}