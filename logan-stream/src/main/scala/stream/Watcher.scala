package stream

import model.ApacheAccessLog
import org.apache.spark.streaming.dstream.DStream

trait Watcher {
  val name: String

  def watch(logs: DStream[ApacheAccessLog])

}
