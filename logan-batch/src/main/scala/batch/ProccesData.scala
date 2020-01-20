package batch

import model.{WebLog, WebReport}
import org.apache.spark.sql.{DataFrame, Dataset}

trait ProccesData {

  def processData(data: Dataset[WebLog]): (Dataset[WebReport], DataFrame)
}
