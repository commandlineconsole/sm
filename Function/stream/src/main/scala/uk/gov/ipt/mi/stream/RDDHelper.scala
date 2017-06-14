package uk.gov.ipt.mi.stream

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.{DateTime, DateTimeZone}

object RDDHelper {

  implicit class RDDPartitionHelper[T <: scala.AnyRef](val dstream: DStream[T]) {

    import org.json4s._
    import org.json4s.jackson.Serialization.write

    def savePartition(basePath: String): Unit = {
      dstream.map { v => {
        implicit val formats = DefaultFormats
        write(v)
      }
      }.foreachRDD { (rdd, Time) =>
        if (!rdd.isEmpty()) {
          val Seq(year, month, day, hour) = getPartitions
          val file = rddToFileName(s"$basePath/year=$year/month=$month/day=$day/hour=$hour/batch", null, Time)
          rdd.saveAsTextFile(file)
        }
      }
    }

    def rddToFileName(prefix: String, suffix: String, time: Time): String = {
      var result = time.milliseconds.toString
      if (prefix != null && prefix.length > 0) {
        result = s"$prefix-$result"
      }
      if (suffix != null && suffix.length > 0) {
        result = s"$result.$suffix"
      }
      result
    }

    def getPartitions: Seq[String] = {
      val time = new DateTime(System.currentTimeMillis(), DateTimeZone.UTC)
      Seq(f"${time.getYear}%04d", f"${time.getMonthOfYear}%02d", f"${time.getDayOfMonth}%02d", f"${time.getHourOfDay}%02d")
    }
  }

}
