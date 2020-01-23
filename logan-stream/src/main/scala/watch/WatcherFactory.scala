package watch

object WatcherFactory {

  def getAllWatchers: List[Watcher] = {
    List(new StatusWatcher,
      new URLWatcher,
      new IPWatcher,
      new NotFoundWatcher,
      new DoSAttack,
      new BruteForceAttack)
  }

  def getCommonWatchers: List[Watcher] = {
    List(new StatusWatcher,
      new URLWatcher,
      new IPWatcher,
      new NotFoundWatcher)
  }

}
