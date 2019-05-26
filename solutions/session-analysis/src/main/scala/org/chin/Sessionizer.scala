package org.chin

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class IPSession(sessionID: String,
                     IP: String,
                     transitionLength: Int,
                     DuringTime: Long,
                     sessionStart:Long,
                     sessionEnd:Long)

/**
  * Sessionizer is a implementation to sessionize input logs by IP-based
  * Refer to: https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#session-windows
  * */
class Sessionizer extends WindowFunction[String, IPSession, String, TimeWindow] {

  val URL_PATTERN = "^.*?(http.*) HTTP.*$".r
  /**
    *  key: IP of session
    *  window: TimeWindow
    *  input: history logs of session
    *  out: Output record
    * */
  def apply(key: String, window: TimeWindow, input: Iterable[String], out: Collector[IPSession]) = {
    var list = ""
    val uniqueUrlsLength = input.map{v =>
      val URL_PATTERN(url) = v
      url
    }.toArray.distinct.length
    val duringTime = window.getEnd - window.getStart
    out.collect(new IPSession(
      sessionID = s"${key}-${window.getStart}-${window.getEnd}",
      IP=key,
      sessionStart = window.getStart,
      sessionEnd = window.getEnd,
      transitionLength= uniqueUrlsLength,
      DuringTime= duringTime
    ))
  }
}
