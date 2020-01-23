package util

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import model.ApacheAccessLog
import org.apache.spark.sql.Row

object ApacheLogParser {

  val datePattern: Pattern = Pattern.compile("\\[(.*?)]")

  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val other = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent $other"
    Pattern.compile(regex)
  }

  def parseDateField(field: String): Option[String] = {
    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")
      val date = dateFormat.parse(dateString)
      val timestamp = new java.sql.Timestamp(date.getTime)
      Option(timestamp.toString)
    } else None
  }

  def parseBytesField(field: String): Option[Long] = {
    try {
      Some(field.toLong)
    } catch {
      case _: Exception => None
    }
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(row: Row): Option[ApacheAccessLog] = {
    val matcher: Matcher = apacheLogPattern().matcher(row.getString(0))
    if (matcher.matches())
      Some(ApacheAccessLog(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        parseBytesField(matcher.group(7)).getOrElse(0),
        matcher.group(8),
        matcher.group(9)
      ))
    else None
  }

  def parseStringLog(log: String): Option[ApacheAccessLog] = {
    val matcher: Matcher = apacheLogPattern().matcher(log)
    if (matcher.matches())
      Some(ApacheAccessLog(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        parseBytesField(matcher.group(7)).getOrElse(0),
        matcher.group(8),
        matcher.group(9)
      ))
    else None
  }
}
