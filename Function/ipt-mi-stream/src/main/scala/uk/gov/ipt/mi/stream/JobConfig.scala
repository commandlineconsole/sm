package uk.gov.ipt.mi.stream

import java.io.{File, FileInputStream}
import java.util.Properties

case class JobConfig(jobConfigFile: File = new File("."))

case class MiStreamConfig(env: String, brokerList: String, batchInterval: Int,
                          offsetReset: String,
                          checkpoint: String,
                          personConfig: MiStreamPersonConfig, identityConfig: MIStreamIdentityConfig,
                          serviceDeliveryConfig: MIStreamServiceDeliveryConfig,
                          serviceDeliveryInvlConfig: MIStreamServiceDeliveryInvlConfig)

case class MIStreamServiceDeliveryConfig(inputTopic: String, hubServiceDeliveryPath: String, sServiceDeliveryInfoPath: String,
                                         lnkParentPath: String, sServiceDeliveryAttributePath: String, sServiceDeliveryProcessPath: String,
                                         sServiceDeliveryProcessInstancePath: String, hubDocAttachmentPath: String, sServiceDeliveryDocAttachmentInfoPath: String,
                                         hubCorrespondencePath: String, sCorrespondenceInfoPath: String)

case class MIStreamServiceDeliveryInvlConfig(hubInvolvementPath:String, sInvolvementInfoPath: String, hubInvolvedPersonPath: String,
                                             sInvolvedPersonInfoPath: String, sInvolvedPersonRecentBiographicsPath: String, lInvolvementPartyPath: String,
                                             hubInvolvementDocAttachmentsPath: String, sInvolvementDocAttachmentsInfoPath: String, hubInvolvementDocAttachCorrespondencePath: String,
                                             sInvolvementDocAttachCorrespondenceInfoPath: String, sInvolvementPOAddressesPath: String, sInvolvementElAddressesPath: String,
                                             sInvolvementNotesPath: String)

case class MIStreamIdentityConfig(inputTopic: String, hubIdentityPath: String,
                                  sIdentityInfoPath: String,
                                  sIdentityBiometricInfoPath: String,
                                  sIdentityReferencePath: String,
                                  sIdentityConditionPath: String,
                                  sIdentityBiographicsPath: String,
                                  sIdentityDescriptorsPath: String,
                                  sIdentityMediasPath: String,
                                  sIdentityLinkPersonPath: String
                                 )

case class MiStreamPersonConfig(inputTopic: String, hubPersonPath: String, personChangeLogPath: String,
                                personVisibilityPath: String, linkPersonPath: String)

object JobConfig {
  def loadConfig(jobConfig: JobConfig): MiStreamConfig = {
    val prop = new Properties()
    prop.load(new FileInputStream(jobConfig.jobConfigFile))

    val personConfig = MiStreamPersonConfig(
      inputTopic = prop.getProperty("mi.stream.person.inputTopic"),
      hubPersonPath = prop.getProperty("mi.stream.personStream.hub.person"),
      personChangeLogPath = prop.getProperty("mi.stream.personStream.satellite.personChangelog"),
      personVisibilityPath = prop.getProperty("mi.stream.personStream.satellite.personVisibility"),
      linkPersonPath = prop.getProperty("mi.stream.personStream.link.personIdentity"))

    val identityConfig = MIStreamIdentityConfig(
      inputTopic = prop.getProperty("mi.stream.identity.inputTopic"),
      hubIdentityPath = prop.getProperty("mi.stream.identityStream.hub.identity"),
      sIdentityInfoPath = prop.getProperty("mi.stream.identityStream.satellite.identityInfo"),
      sIdentityBiometricInfoPath = prop.getProperty("mi.stream.identityStream.satellite.identityBiometricInfo"),
      sIdentityReferencePath = prop.getProperty("mi.stream.identityStream.satellite.identityReference"),
      sIdentityConditionPath = prop.getProperty("mi.stream.identityStream.satellite.identityCondition"),
      sIdentityBiographicsPath = prop.getProperty("mi.stream.identityStream.satellite.identityBiographics"),
      sIdentityDescriptorsPath = prop.getProperty("mi.stream.identityStream.satellite.identityDescriptors"),
      sIdentityMediasPath = prop.getProperty("mi.stream.identityStream.satellite.identityMedias"),
      sIdentityLinkPersonPath = prop.getProperty("mi.stream.identityStream.link.identityPerson")
    )

    val serviceDeliveryConfig = MIStreamServiceDeliveryConfig(
      inputTopic = prop.getProperty("mi.stream.servicedelivery.inputTopic"),
      hubServiceDeliveryPath = prop.getProperty("mi.stream.servicedeliveryStream.hub.servicedelivery"),
      sServiceDeliveryInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInfo"),
      lnkParentPath = prop.getProperty("mi.stream.servicedeliveryStream.link.sdParent"),
      sServiceDeliveryAttributePath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdAttribute"),
      sServiceDeliveryProcessPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdProcess"),
      sServiceDeliveryProcessInstancePath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdProcessInstance"),
      hubDocAttachmentPath = prop.getProperty("mi.stream.servicedeliveryStream.hub.sdDocAttachment"),
      sServiceDeliveryDocAttachmentInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdDocAttachmentInfo"),
      hubCorrespondencePath = prop.getProperty("mi.stream.servicedeliveryStream.hub.sdDocAttachmentCorr"),
      sCorrespondenceInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdDocAttachmentCorrInfo")
    )

    val serviceDeliveryInvlConfig = MIStreamServiceDeliveryInvlConfig(
      hubInvolvementPath = prop.getProperty("mi.stream.servicedeliveryStream.hub.sdInvolvement"),
      sInvolvementInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvolvementInfo"),
      hubInvolvedPersonPath = prop.getProperty("mi.stream.servicedeliveryStream.hub.sdInvolvedPerson"),
      sInvolvedPersonInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvolvedPersonInfo"),
      sInvolvedPersonRecentBiographicsPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvolvedPersonRecentBiographics"),
      lInvolvementPartyPath = prop.getProperty("mi.stream.servicedeliveryStream.link.sdInvlParty"),
      hubInvolvementDocAttachmentsPath = prop.getProperty("mi.stream.servicedeliveryStream.hub.sdInvlDocAttachment"),
      sInvolvementDocAttachmentsInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvlDocAttachmentInfo"),
      hubInvolvementDocAttachCorrespondencePath = prop.getProperty("mi.stream.servicedeliveryStream.hub.sdInvlDocAttachmentCorr"),
      sInvolvementDocAttachCorrespondenceInfoPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvlDocAttachmentCorrInfo"),
      sInvolvementPOAddressesPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvlPOAddress"),
      sInvolvementElAddressesPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvlElAddress"),
      sInvolvementNotesPath = prop.getProperty("mi.stream.servicedeliveryStream.satellite.sdInvlNotes=/apps/MI/stream/servicedelivery/satellite/sdInvlNotes")

    )

    MiStreamConfig(
      env = prop.getProperty("mi.stream.env"),
      brokerList = prop.getProperty("metadata.broker.list"),
      batchInterval = prop.getProperty("mi.stream.interval.seconds").toInt,
      offsetReset = prop.getProperty("kafka.auto.offset.reset", "largest"),
      checkpoint = prop.getProperty("mi.stream.checkpoint"),
      personConfig = personConfig,
      identityConfig = identityConfig,
      serviceDeliveryConfig = serviceDeliveryConfig,
      serviceDeliveryInvlConfig = serviceDeliveryInvlConfig)
  }
}
