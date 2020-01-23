package watch

import model.ApacheAccessLog
import org.apache.spark.streaming.dstream.DStream

trait Watcher {
  val path: String

  def watch(logs: DStream[ApacheAccessLog], persist: Boolean)

}
