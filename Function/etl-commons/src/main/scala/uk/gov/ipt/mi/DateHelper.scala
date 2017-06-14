package uk.gov..mi

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.Option._

object DateHelper {

  /**
   *
   * @param datesList an input list containing each of the dates as scala Options
   * @return the most recent date as a String using ISODateTimeFormat, or an one if the list contains only empty Options
   */
  def getMostRecentDate(datesList: List[Option[String]]): Option[String] = {
    def getTime(dateStr: Option[String]) = {
      if (dateStr.isDefined) {
        try {
          Some(DateTime.parse(dateStr.get).getMillis)
        } catch {
          case e: Exception => empty
        }
      }
      else empty
    }
    datesList.flatMap(getTime).sorted(Ordering[Long].reverse).headOption.map(ISODateTimeFormat.dateTime().print)
  }

}
