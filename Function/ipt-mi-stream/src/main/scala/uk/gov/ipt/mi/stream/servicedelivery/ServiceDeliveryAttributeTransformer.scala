package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.SServiceDeliveryAttribute
import uk.gov.ipt.mi.model.servicedelivery.{ServiceDelivery, ServiceDeliveryAttribute}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper._


object ServiceDeliveryAttributeTransformer {

  def attributes(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryAttribute] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val attr_list_agg_str = attributesSetStr(serviceDelivery.attributes)
    val attr_list_agg_hash = HashHelper.sha1(attr_list_agg_str)
    val lnk_idntty_condition_list_hk = HashHelper.sha1(List(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)) ++ attr_list_agg_str)

    val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))

    serviceDelivery.attributes.sorted.zipWithIndex.map{case (attribute: ServiceDeliveryAttribute, i: Int) => {
      val rec_hash_value = HashHelper.sha1(attributeStr(attribute))

      SServiceDeliveryAttribute(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        source, serviceDelivery.internalHandle.interface_identifier, attribute.internalHandle.interface_identifier,
        i, rec_hash_value, attribute.internalHandle.visibility_marker,
        attribute.serviceDeliveryAttributeTypeCode, attribute.attributeValueDate, attribute.attributeValueString,
        attribute.attributeValueNumber, attribute.attributeValueRefDataElement.map(_.code), attribute.createdBy,
        attribute.created, attr_list_agg_hash, lnk_idntty_condition_list_hk,
        srvc_dlvry_hk)
    }}
  }

  def attributeStr(attribute: ServiceDeliveryAttribute): List[(String, String)] = {
    List(
      ("attr_handle_visibility", attribute.internalHandle.visibility_marker.getOrElse("")),
      ("attr_type_cd", attribute.serviceDeliveryAttributeTypeCode.getOrElse("")),
      ("attr_value_datetime", attribute.attributeValueDate.getOrElse("")),
      ("attr_value_string", attribute.attributeValueString.getOrElse("")),
      ("attr_value_number", attribute.attributeValueNumber.map(_.toString).getOrElse("")),
      ("attr_ref_data_cd", attribute.attributeValueRefDataElement.map(_.code).getOrElse(""))
    )
  }

  def attributesSetStr(attributes: List[ServiceDeliveryAttribute]): List[(String, String)] = {
    attributes.sorted.flatMap(attribute =>
      List(("attr_handle_id", attribute.internalHandle.interface_identifier)) ++ attributeStr(attribute))
  }

}
