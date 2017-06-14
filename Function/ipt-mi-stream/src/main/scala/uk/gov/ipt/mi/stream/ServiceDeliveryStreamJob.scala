package uk.gov.ipt.mi.stream

import org.apache.spark.streaming.dstream.DStream
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.servicedelivery._
import RDDHelper._

class ServiceDeliveryStreamJob() {

  def start(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], serviceDeliveryConfig: MIStreamServiceDeliveryConfig): Unit = {

    streamHubServiceDelivery(serviceDeliveryStream, serviceDeliveryConfig.hubServiceDeliveryPath)
    streamSServiceDeliveryInfo(serviceDeliveryStream, serviceDeliveryConfig.sServiceDeliveryInfoPath)
    streamLinkSvcDeliveryParent(serviceDeliveryStream, serviceDeliveryConfig.lnkParentPath)
    streamSServiceDeliveryAttribute(serviceDeliveryStream, serviceDeliveryConfig.sServiceDeliveryAttributePath)
    streamSServiceDeliveryProcess(serviceDeliveryStream, serviceDeliveryConfig.sServiceDeliveryProcessPath)
    streamSServiceDeliveryProcessInstance(serviceDeliveryStream, serviceDeliveryConfig.sServiceDeliveryProcessInstancePath)
    streamHubDocAttachement(serviceDeliveryStream, serviceDeliveryConfig.hubDocAttachmentPath)
    streamSSDocAttachementInfo(serviceDeliveryStream, serviceDeliveryConfig.sServiceDeliveryDocAttachmentInfoPath)
    streamCorrespondenceHub(serviceDeliveryStream, serviceDeliveryConfig.hubCorrespondencePath)
    streamSCorrespondenceInfo(serviceDeliveryStream, serviceDeliveryConfig.sCorrespondenceInfoPath)
  }

  def streamHubServiceDelivery(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.map{ case(messageId: String, serviceDelivery: ServiceDelivery, timestamp:Long) =>
        ServiceDeliveryHubTransformer.serviceDeliveryHub(messageId,serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamSServiceDeliveryInfo(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.map{ case(messageId: String, serviceDelivery: ServiceDelivery, timestamp:Long) =>
      ServiceDeliveryInfoTransformer.serviceDeliveryInfo(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamLinkSvcDeliveryParent(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
        ServiceDeliveryLinkParentTransformer.linkParent(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamSServiceDeliveryAttribute(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
        ServiceDeliveryAttributeTransformer.attributes(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamSServiceDeliveryProcess(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
        ServiceDeliveryProcessTransformer.process(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamSServiceDeliveryProcessInstance(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      ServiceDeliveryProcessInstanceTransformer.processInstances(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamHubDocAttachement(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      ServiceDeliveryHubDocAttachTransformer.hubDocAttachment(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }


  def streamSSDocAttachementInfo(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      ServiceDeliveryDocAttachmentInfo.docAttachmentInfos(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }

  def streamCorrespondenceHub(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      ServiceDeliveryHubCorrespondenceTransformer.hubCorrespondence(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }


  def streamSCorrespondenceInfo(serviceDeliveryStream: DStream[(String, ServiceDelivery, Long)], basePath:String): Unit = {
    serviceDeliveryStream.flatMap { case (messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) =>
      ServiceDeliveryCorrInfoTransformer.correspondenceInfo(messageId, serviceDelivery, timestamp)
    }.savePartition(basePath)
  }


}
