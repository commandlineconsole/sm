package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

object ClockWrapper {

  def setTime(ssc: StreamingContext, timeToSet: Duration): Unit = {
    val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    manualClock.setTime(timeToSet.milliseconds)
  }

  def advance(ssc: StreamingContext, timeToAdd: Duration): Unit = {
    val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    manualClock.advance(timeToAdd.milliseconds)
  }
}
