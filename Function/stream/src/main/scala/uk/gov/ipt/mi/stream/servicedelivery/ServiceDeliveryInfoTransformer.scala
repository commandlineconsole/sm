package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.SServiceDeliveryInfo
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper._


object ServiceDeliveryInfoTransformer {

  def serviceDeliveryInfo(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): SServiceDeliveryInfo = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))

    val chlg_rec_hash_value = sha1(Seq(
      ("message_id", messageId),
      ("src_created_by", serviceDelivery.createdUserId.getOrElse("")),
      ("src_last_updated_by", serviceDelivery.lastUpdatedUserId.getOrElse(""))))

    val vis_rec_hash_value = sha1(serviceDelivery.internalHandle.visibility_marker.getOrElse(""))

    val eh_rec_hash_value = sha1(Seq(
      ("external_handle_value", serviceDelivery.externalHandleValue.getOrElse("")),
      ("external_handle_space", serviceDelivery.externalHandleSpace.getOrElse(""))))

    val main_rec_hash_value = sha1(Seq(
      ("srvc_dlvry_type_cd", serviceDelivery.serviceDeliveryType.refDataValueCode),
      ("due_date", serviceDelivery.dueDate.getOrElse("")),
      ("priority", serviceDelivery.priority.map(_.toString).getOrElse("")),
      ("primary_ref_type_cd", serviceDelivery.primaryRefType.map(_.refDataValueCode).getOrElse("")),
      ("primary_ref_value", serviceDelivery.primaryRefValue.getOrElse(""))
    ))
    val status_rec_hash_value = sha1(serviceDelivery.serviceDeliveryStatus.refDataValueCode)

    SServiceDeliveryInfo(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
      source, serviceDelivery.internalHandle.interface_identifier, chlg_rec_hash_value,
      serviceDelivery.createdUserId, serviceDelivery.created, serviceDelivery.lastUpdatedUserId,
      serviceDelivery.lastUpdated, serviceDelivery.createdDate, vis_rec_hash_value,
      serviceDelivery.internalHandle.visibility_marker, eh_rec_hash_value, serviceDelivery.externalHandleValue,
      serviceDelivery.externalHandleSpace, main_rec_hash_value, serviceDelivery.serviceDeliveryType.refDataValueCode,
      serviceDelivery.dueDate, serviceDelivery.priority, serviceDelivery.primaryRefType.map(_.refDataValueCode),
      serviceDelivery.primaryRefValue, status_rec_hash_value, serviceDelivery.serviceDeliveryStatus.refDataValueCode,
      srvc_dlvry_hk)
  }
}
