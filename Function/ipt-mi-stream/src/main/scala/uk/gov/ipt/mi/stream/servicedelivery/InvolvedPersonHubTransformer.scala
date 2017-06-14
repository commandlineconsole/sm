package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.HubServiceDeliveryInvolvedPerson
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.HashHelper

object InvolvedPersonHubTransformer {

  def involvedPersonHub(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) : List[HubServiceDeliveryInvolvedPerson] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.involvements.filter(_.person.isDefined).sorted.map(involvement => {
      val involved_person_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("involved_person_id", involvement.person.map(_.personId).getOrElse(""))
      ))
      HubServiceDeliveryInvolvedPerson(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        involved_person_hk, source, serviceDelivery.internalHandle.interface_identifier,
        involvement.person.map(_.personId))
    })

  }
}
