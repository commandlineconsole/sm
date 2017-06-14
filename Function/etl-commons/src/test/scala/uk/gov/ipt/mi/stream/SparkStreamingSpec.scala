package uk.gov..mi.stream

import java.io.File

import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.scalatest.Suite
import org.apache.spark.streaming.ClockWrapper
import uk.gov..mi.SparkSpec

trait SparkStreamingSpec extends SparkSpec {
  this: Suite =>

  var ssc: StreamingContext = _

  def batchDuration:Duration = Seconds(1)

  override def beforeAll: Unit = {
    super.beforeAll

    ssc = new StreamingContext(spark.sparkContext, batchDuration)
  }

  override def afterAll: Unit = {
    if (ssc !=null){
      ssc.stop(stopSparkContext = false, stopGracefully = false)
      ssc = null
    }
    super.afterAll
  }

  override def sparkConfig: Map[String, String] = {
    super.sparkConfig + ("spark.streaming.clock" -> "org.apache.spark.streaming.util.ManualClock")
  }

  def advanceClock(timeToAdd: Duration): Unit = {
    ClockWrapper.advance(ssc, timeToAdd)
  }

  def setClockTime(timeToSet: Duration): Unit = {
    ClockWrapper.setTime(ssc,timeToSet)
  }

  def advanceClockOneBatch(): Unit = {
    advanceClock(Duration(batchDuration.milliseconds))
  }

  def deleteFile(file: File): Unit = {
    if (!file.exists) return
    if (file.isFile) {
      file.delete()
    } else {
      file.listFiles().foreach(deleteFile)
      file.delete()
    }
  }
}
