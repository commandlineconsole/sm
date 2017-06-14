package uk.gov.ipt.mi.stream

import org.apache.spark.streaming.dstream.DStream
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.servicedelivery._
import RDDHelper._

class ServiceDeliveryInvolvementStreamJob {

  def start(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], serviceDeliveryInvolvementConfig: MIStreamServiceDeliveryInvlConfig): Unit = {
    streamHubInvolvement(serviceDeliveryStream, serviceDeliveryInvolvementConfig.hubInvolvementPath)
    streamInvolvementInfo(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvementInfoPath)
    streamInvolvedPerson(serviceDeliveryStream, serviceDeliveryInvolvementConfig.hubInvolvedPersonPath)
    streamInvolvedPersonInfo(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvedPersonInfoPath)
    streamInvolvedPersonRecentBiographics(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvedPersonRecentBiographicsPath)
    streamInvolvementPartyLinks(serviceDeliveryStream, serviceDeliveryInvolvementConfig.lInvolvementPartyPath)
    streamHubInvolvementDocAttachments(serviceDeliveryStream, serviceDeliveryInvolvementConfig.hubInvolvementDocAttachmentsPath)
    streamInvolvementDocAttachmentsInfo(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvementDocAttachmentsInfoPath)
    streamHubInvolvementDocAttachmentCorrespondences(serviceDeliveryStream, serviceDeliveryInvolvementConfig.hubInvolvementDocAttachCorrespondencePath)
    streamHubInvolvementDocAttachmentCorrespondenceInfos(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvementDocAttachCorrespondenceInfoPath)
    streamInvolvementPOAddresses(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvementPOAddressesPath)
    streamInvolvementElAddresses(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvementElAddressesPath)
    streamInvolvementNotes(serviceDeliveryStream, serviceDeliveryInvolvementConfig.sInvolvementNotesPath)
  }

  def streamHubInvolvement(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap{ case(messageId: String, serviceDelivery: ServiceDelivery, timestamp:Long) =>
      InvolvementHubTransformer.involvementHub(messageId,serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvementInfo(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap{ case(messageId: String, serviceDelivery: ServiceDelivery, timestamp:Long) =>
      SInvolvementInfoTransformer.involvementInfo(messageId,serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvedPerson(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      InvolvedPersonHubTransformer.involvedPersonHub(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvedPersonInfo(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvedPersonInfoTransformer.involvedPersonInfo(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvedPersonRecentBiographics(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvedPersonRecentBiographicsTransformer.involvedPersonRecentBiographics(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvementPartyLinks(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      LInvolvementPartyTransformer.involvementLinkParty(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamHubInvolvementDocAttachments(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvementHubDocAttachmentTransformer.hubDocAttachment(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvementDocAttachmentsInfo(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvementDocAttachmentInfoTransformer.docAttachment(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamHubInvolvementDocAttachmentCorrespondences(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvementHubCorrespondenceTransformer.hubCorrespondence(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamHubInvolvementDocAttachmentCorrespondenceInfos(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SinvolvementDocAttachmentCorrespondenceInfoTransformer.correspondenceInfo(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvementPOAddresses(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvementPostalAddressTransformer.involvementPostalAddresses(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvementElAddresses(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvementElectronicAddressTransformer.involvementElectronicAddresses(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamInvolvementNotes(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath: String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      SInvolvementNoteTransformer.involvementNotes(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }
}
