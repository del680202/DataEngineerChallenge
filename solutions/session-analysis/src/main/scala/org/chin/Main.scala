package org.chin

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * Flink Job for session analysis.
  * @input:  log path
  * @output: Folder of result output
  */
object Main {

  val DATE_PATTERN = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}".r
  val DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
  val IP_PATTERN = "^[-0-9:TZ.]+ \\S+ ([0-9a-z:.]+):\\d+ .*$".r
  val SESSION_GAP_LENGTH = 10 // 10 mins

  def getLogTimestamp(log:String): Long ={
    val format = new java.text.SimpleDateFormat(DATE_FORMAT)
    return DATE_PATTERN.findFirstIn(log) match {
      case Some(dateString) => format.parse(dateString).getTime()
      case None => 0
    }
  }

  def main(args: Array[String]): Unit = {

    // For implicit conversion
    import org.apache.flink.streaming.api.scala._
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val output = params.getRequired("output")

    val env =  StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1)


    val text = env.readTextFile(input)
    // Convert log date to event timestamp
    val timestamped = text.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String]{
      override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
        return getLogTimestamp(element)
      }
      override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
        val timestamp = getLogTimestamp(lastElement)
        return if(timestamp <= 0){
          null
        }else{
          new Watermark(timestamp)
        }
      }
    })


    timestamped.keyBy{v =>
      val targetIP = v match{
        case IP_PATTERN(ip) => ip
        case _ => "UnknownIP"
      }
      targetIP
     }.window(EventTimeSessionWindows.withGap(Time.minutes(SESSION_GAP_LENGTH)))
      .apply(new Sessionizer())
      .writeAsCsv(output, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

    env.execute("Sessionizer Task")
  }
}


