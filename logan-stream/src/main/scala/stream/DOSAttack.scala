package stream
import model.ApacheAccessLog
import org.apache.spark.streaming.dstream.DStream

class DOSAttack extends Watcher {

  override val name: String = "DOSAttack"

  override def watch(logs: DStream[ApacheAccessLog]): Unit = {

  }
}
