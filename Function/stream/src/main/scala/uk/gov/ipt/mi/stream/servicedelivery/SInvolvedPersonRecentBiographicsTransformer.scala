package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.SServiceDeliveryInvolvedPersonRecentBiographics
import uk.gov.ipt.mi.model.servicedelivery.{Biographic, ServiceDelivery}
import uk.gov.ipt.mi.stream.HashHelper

object SInvolvedPersonRecentBiographicsTransformer {

  def involvedPersonRecentBiographics(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryInvolvedPersonRecentBiographics] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.involvements.filter(_.person.isDefined).sorted.flatMap(involvement => {
      val recent_biog_list_agg_hash = recentBiographicsHash(involvement.person.map(_.recentBiographics).getOrElse(Set()))
      val involved_person_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("involved_person_id", involvement.person.map(_.personId).getOrElse(""))
      ))
      involvement.person.map(_.recentBiographics.zipWithIndex.map {
        case (biographic: Biographic, index: Int) =>
          SServiceDeliveryInvolvedPersonRecentBiographics(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
            source, serviceDelivery.internalHandle.interface_identifier, involvement.person.map(_.personId),
            biographic.internalHandle.map(_.interface_identifier), index, HashHelper.sha1(biographicStr(biographic)),
            biographic.internalHandle.flatMap(_.visibility_marker), biographic.biographicType, biographic.biographicValue,
            recent_biog_list_agg_hash, involved_person_hk)
      }).getOrElse(Set())
    })
  }


  private def recentBiographicsHash(recentBiographics: Set[Biographic]): String = {
    HashHelper.sha1(recentBiographicsAggStr(recentBiographics))
  }

  private def recentBiographicsAggStr(recentBiographics: Set[Biographic]): List[(String, String)] = {
    recentBiographics.toList.sorted.flatMap { biographic: Biographic =>
      biographicStr(biographic)
    }
  }

  private def biographicStr(biographic: Biographic): List[(String, String)] = {
    List(
      ("biographic_type_cd", biographic.biographicType),
      ("biographic_value", biographic.biographicValue))
  }

}
