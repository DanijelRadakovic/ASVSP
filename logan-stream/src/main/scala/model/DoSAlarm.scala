package model

import java.sql.Timestamp

case class DoSAlarm(timestamp: Timestamp, errors: Long, message:String)
