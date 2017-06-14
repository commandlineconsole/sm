package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov..mi.model.SServiceDeliveryInvolvedPersonInfo
import uk.gov..mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class InvolvedPersonInfoTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement" should "have direct involvement involved person infos " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvedPersonInfos: List[SServiceDeliveryInvolvedPersonInfo] = SInvolvedPersonInfoTransformer.involvedPersonInfo("messageId", serviceDelivery, System.currentTimeMillis())

    involvedPersonInfos should have size serviceDelivery.involvements.size

    val involvedPersonInfo = involvedPersonInfos.head
    involvedPersonInfo.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    involvedPersonInfo.default_biog_rec_hash_value should not equal HashHelper.emptyHash
    involvedPersonInfo.involved_person_id should equal(serviceDelivery.involvements.sorted.head.person.map(_.personId))
    involvedPersonInfo.involved_person_created_by should equal(serviceDelivery.involvements.sorted.head.person.flatMap(_.createdUserId))
    involvedPersonInfo.involved_person_created_dtime should equal(serviceDelivery.involvements.sorted.head.person.flatMap(_.createdDate))

  }
}
