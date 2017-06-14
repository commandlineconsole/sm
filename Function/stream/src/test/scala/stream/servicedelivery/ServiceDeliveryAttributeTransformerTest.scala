package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov..mi.model.SServiceDeliveryAttribute
import uk.gov..mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class ServiceDeliveryAttributeTransformerTest extends FlatSpec with Matchers with Inside {

  "Service delivery attributes " should "return correct Attribute list" in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val actualAttributes = ServiceDeliveryAttributeTransformer.attributes("messageId", serviceDelivery, System.currentTimeMillis())

    actualAttributes should have size serviceDelivery.attributes.size

    inside(actualAttributes.head){
      case SServiceDeliveryAttribute(_, _, _,
      _, srvc_dlvry_handle_id, attr_handle_id,
      rec_seqno, rec_hash_value, attr_handle_visibility,
      attr_type_cd, attr_value_datetime, attr_value_string,
      attr_value_number, attr_ref_data_cd, attr_created_by,
      attr_created_datetime, attr_list_agg_hash, lnk_srvcdlvry_attr_list_hk,
      srvc_dlvry_hk) =>
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        val expectedAttribute = serviceDelivery.attributes.sorted.head
        attr_handle_id should equal(expectedAttribute.internalHandle.interface_identifier)
        rec_seqno should equal(0)
        rec_hash_value should not equal HashHelper.emptyHash
        attr_handle_visibility should equal(expectedAttribute.internalHandle.visibility_marker)
        attr_type_cd should equal(expectedAttribute.serviceDeliveryAttributeTypeCode)
        attr_value_datetime should equal(expectedAttribute.attributeValueDate)
        attr_value_string should equal(expectedAttribute.attributeValueString)
        attr_value_number should equal(expectedAttribute.attributeValueNumber)
        attr_ref_data_cd should equal(expectedAttribute.attributeValueRefDataElement.map(_.code))
        attr_created_by should equal(expectedAttribute.createdBy)
        attr_created_datetime should equal(expectedAttribute.created)
        attr_list_agg_hash should not equal HashHelper.emptyHash
        lnk_srvcdlvry_attr_list_hk should not equal HashHelper.emptyHash
        srvc_dlvry_hk should not equal HashHelper.emptyHash
    }
  }
}
