package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}


@RunWith(classOf[JUnitRunner])
class ServiceDeliveryCorrInfoTransformerTest extends FlatSpec with Matchers with Inside {

  "Service delivery correspondence Info" should "return list correspondence info " in {
    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val actualCorrespondence = ServiceDeliveryCorrInfoTransformer.correspondenceInfo("messageId", serviceDelivery, System.currentTimeMillis())

    actualCorrespondence should have size serviceDelivery.documentAttachments.flatMap(_.correspondences.map(_.size)).sum

  }

}
