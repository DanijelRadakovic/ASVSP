package model

case class GeneralWebReport(hits: Long,
                            successfulRequests: Long,
                            failedRequest: Long,
                            response404: Long,
                            visitors: Long,
                            txAmount: Long,
                            noReferer: Long)
