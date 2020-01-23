package stream

object WatcherFactory {

  def getAllWatchers: List[Watcher] = {
    List(new StatusWatcher,
      new IPAddressWatcher,
      new URLWatcher)
  }

  def getCommonWatchers: List[Watcher] = {
    List(new StatusWatcher,
      new IPAddressWatcher,
      new URLWatcher)
  }

}
