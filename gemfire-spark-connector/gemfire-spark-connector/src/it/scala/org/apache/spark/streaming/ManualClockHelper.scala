package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

object ManualClockHelper {

  def addToTime(ssc: StreamingContext, timeToAdd: Long): Unit = {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    clock.advance(timeToAdd)
  }
  
}
