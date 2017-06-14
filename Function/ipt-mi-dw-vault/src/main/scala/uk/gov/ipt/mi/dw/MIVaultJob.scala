package uk.gov.ipt.mi.dw

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import uk.gov.ipt.mi.dw.person.PersonVaultJob
import uk.gov.ipt.mi.dw.identity.IdentityHubVaultJob
import uk.gov.ipt.mi.dw.identity.IdentitySatVaultJob
import uk.gov.ipt.mi.dw.servicedelivery.SrvcDlvryBaseVaultJob

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;


object MIVaultJob {
  val log = LoggerFactory.getLogger(MIVaultJob.this.getClass)

  def main(args: Array[String]) : Unit = {

    val parser = new scopt.OptionParser[JobConfig]("scopt") {
      opt[File]('c', "job-config") required() valueName "<confFile>" action { (x, c) =>
        c.copy(jobConfigFile = x)
      } text "job config location"
    }

    parser.parse(args, JobConfig()).map{ jobConfig =>
      MIVaultJob.start(JobConfig.loadConfig(jobConfig))
    }
  }

  def start(config: MIVaultConfig): Unit = {
    log.info("[MIVaultJob] starting...")
    val sparkConf = new SparkConf().setAppName("MIVaultJob")

    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    val batchRunDateFormat = new SimpleDateFormat("yyyy-MM-dd hh.mm.ss");
    val currSystemDateTime = Calendar.getInstance().getTime
    val currBatchId = "Batch" + batchIdDateFormat.format(currSystemDateTime )
    val currBatchRunDateTime = batchRunDateFormat.format(currSystemDateTime )

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
/*
    val personVaultJob = new PersonVaultJob("batchId" ,"timestamp", spark)

    val identityHubVaultJob = new IdentityHubVaultJob("batchId" ,"timestamp", spark)

    val identitySatVaultJob = new IdentitySatVaultJob("batchId" ,"timestamp", spark)

    val srvcDlvryHubVaultJob = new SrvcDlvryHubVaultJob("batchId", "timestamp", spark)
*/

    val srvcDlvryBaseVaultJob = new SrvcDlvryBaseVaultJob(batchId = currBatchId,
                                                          timestampStr = currBatchRunDateTime,
                                                          spark = spark)
/*
    log.info("[MIVaultJob] start person VAULT")
    personVaultJob.start(config.dbConfig, config.personVaultConfig)
    log.info("[MIVaultJob] person VAULT completed")

    log.info("[MIVaultJob] start identity hub VAULT")
    identityHubVaultJob.start(config.dbConfig, config.identityHubVaultConfig)
    log.info("[MIVaultJob] identity hub VAULT completed ")
*/
/*
    log.info("[MIVaultJob] start servicedelivery hub VAULT")
    srvcDlvryHubVaultJob.start(config.dbConfig, config.srvcDlvryHubVaultConfig)
    log.info("[MIVaultJob] servicedelivery hub VAULT completed ")
*/
/*
    log.info("[MIVaultJob] start identity Satellite VAULT")
    identitySatVaultJob.start(config.dbConfig, config.identitySatVaultConfig)
    log.info("[MIVaultJob] identity Satellite VAULT completed ")
*/
    log.info("[MIVaultJob] Starting ServiceDelivery Base Vault [Hub + Satellites] Job...")
    srvcDlvryBaseVaultJob.start(config.dbConfig, config.srvcDlvryHubVaultConfig, config.srvcDlvrySatVaultConfig)
    log.info("[MIVaultJob] ServiceDelivery Base Vault [Hub + Satellites] Job Completed ")

    log.info("[MIVaultJob] terminated")
  }
}
