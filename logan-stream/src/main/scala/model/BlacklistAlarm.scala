package model

import java.sql.Timestamp

case class BlacklistAlarm(timestamp: Timestamp, ip: String, errors: Long, message: String)
