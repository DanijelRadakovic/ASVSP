package batch

object ProcessDataResolver {

  private val commandMap = Map(
    "status" -> new StatusReport,
    "ip" -> new IPReport,
    "doc" -> new DocumentReport,
    "datetime" -> new DateTimeReport,
    "not-found" -> new NotFoundResponseReport,
    "general" -> new GeneralReport
  )

  def factory(command: String): Option[ProccesData] = {
      commandMap.get(command)
  }

}
