package uk.gov..mi

import java.io.File

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.slf4j.LoggerFactory
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.model.{Identity, Person}
import uk.gov..mi.stream._

import scala.util.{Failure, Success, Try}


object MIStreamJob {

  val log = LoggerFactory.getLogger(MIStreamJob.this.getClass)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[JobConfig]("scopt") {
      opt[File]('c', "job-config") required() valueName "<confFile>" action { (x, c) =>
        c.copy(jobConfigFile = x)
      } text "job config location"
    }

    parser.parse(args, JobConfig()).map { jobConfig =>
      (MIStreamJob.start(JobConfig.loadConfig(jobConfig)))
    }
  }

  implicit val formats = DefaultFormats // Brings in default date formats etc.

  def getFromOffsets(
                      kc: KafkaCluster,
                      kafkaParams: Map[String, String],
                      topics: Set[String]
                    ): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }

  def streamingContext(miStreamConfig: MiStreamConfig): StreamingContext = {

    val sparkConf = new SparkConf().setAppName("MIStreamJob")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(miStreamConfig.batchInterval))

    ssc

    // Create context with miStreamConfig.batchInterval
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> miStreamConfig.brokerList,
      "auto.offset.reset" -> miStreamConfig.offsetReset
    )
    val kc = new KafkaCluster(kafkaParams)


    val personTopicoffsets = getFromOffsets(kc, kafkaParams, Set(miStreamConfig.personConfig.inputTopic))
    personTopicoffsets.foreach { case (topicAndParition: TopicAndPartition, offest: Long) =>
      log.info(s"person topic [${miStreamConfig.personConfig.inputTopic}] partition[${topicAndParition.partition},$offest]")
    }
    //person
    val jsonPersonStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, scala.Int, scala.Long, String)](ssc, kafkaParams, personTopicoffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.partition, mmd.offset, mmd.message))

    val personStream = jsonPersonStream
      .transform((rdd, time) => rdd.map((time, _)))
      .map { case (time: Time, (messageId: String, partitionId: Int, offset: Long, pJson: String)) =>
        (messageId + "|" +  offset, Try(parse(pJson).extract[Person]) match {
          case Success(person) => person
          case Failure(f) => log.error(s"unable to parse person message $messageId-$pJson ", f)
            throw new MappingException(s"unable to parse person message $messageId-$pJson ")
        }, time.milliseconds)
      }

    val personStreamJob = new PersonStreamJob()

    personStreamJob.start(personStream, miStreamConfig.personConfig)

    //Identity
    val identityTopicOffsets = getFromOffsets(kc, kafkaParams, Set(miStreamConfig.identityConfig.inputTopic))

    identityTopicOffsets.foreach { case (topicAndParition: TopicAndPartition, offest: Long) =>
      log.info(s"identity topic [${miStreamConfig.identityConfig.inputTopic}] partition[${topicAndParition.partition},$offest]")
    }


    val jsonIdentityStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, scala.Int, scala.Long, String)](ssc, kafkaParams, identityTopicOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.partition, mmd.offset, mmd.message))


    val identityStream = jsonIdentityStream
      .transform((rdd, time) => rdd.map((time, _)))
      .map { case (time: Time, (messageId: String, partitionId: Int, offset: Long, identityJson: String)) =>
        (messageId + "|" +  offset, Try(parse(identityJson).extract[Identity]) match {
          case Success(identity) => identity
          case Failure(f) => log.error(s"unable to parse identity message $messageId-$identityJson ", f)
            throw new MappingException(s"unable to parse identity message $messageId-$identityJson ")
        }, time.milliseconds)
      }

    val identityStreamJob = new IdentityStreamJob()

    identityStreamJob.start(identityStream, miStreamConfig.identityConfig)

    //ServiceDelivery
    val serviceDeliveryTopicOffsets = getFromOffsets(kc, kafkaParams, Set(miStreamConfig.serviceDeliveryConfig.inputTopic))

    serviceDeliveryTopicOffsets.foreach { case (topicAndParition: TopicAndPartition, offset: Long) =>
      log.info(s"ServiceDelivery topic [${miStreamConfig.serviceDeliveryConfig.inputTopic}] partition[${topicAndParition.partition},$offset]")
    }

    val jsonServiceDeliveryStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, scala.Int, scala.Long, String)](ssc, kafkaParams, serviceDeliveryTopicOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.partition, mmd.offset, mmd.message))

    val serviceDeliveryStream = jsonServiceDeliveryStream
      .transform((rdd, time) => rdd.map((time, _)))
      .map { case (time: Time, (messageId: String, partitionId: Int, offset: Long, svcDlvJson: String)) =>
        (messageId + "|" +  offset, Try(parse(svcDlvJson).extract[ServiceDelivery]) match {
          case Success(serviceDelivery) => serviceDelivery
          case Failure(f) => log.error(s"unable to parse service delivery message $messageId-$svcDlvJson ", f)
            throw new MappingException(s"unable to parse service delivery message $messageId-$svcDlvJson ")
        }, time.milliseconds)
      }

    val serviceDeliveryStreamJob = new ServiceDeliveryStreamJob()

    serviceDeliveryStreamJob.start(serviceDeliveryStream, miStreamConfig.serviceDeliveryConfig)

    val serviceDeliveryInvolvementStreamJob = new ServiceDeliveryInvolvementStreamJob()

    serviceDeliveryInvolvementStreamJob.start(serviceDeliveryStream, miStreamConfig.serviceDeliveryInvlConfig)

    ssc
  }

  def start(miStreamConfig: MiStreamConfig): Unit = {
    log.info("[MIStreamJob] starting...")
    // Create context with x batch seconds interval

    val ssc = StreamingContext.getActiveOrCreate(() => streamingContext(miStreamConfig))

    //start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
