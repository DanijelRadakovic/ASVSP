package model

case class ApacheAccessLog(ip:String, client:String, user:String, dateTime:String, request:String, status:String,
                           bytes:Long, referer:String, agent:String)
