package model

import java.sql.Timestamp

case class WebLog(ip: String,
                  cik: String,
                  accession: String,
                  extension: String,
                  code: Int,
                  size: Int,
                  idx: Int,
                  noRefer: Int,
                  noAgent: Int,
                  find: Int,
                  crawler: Int,
                  datetime: Timestamp) {

}
