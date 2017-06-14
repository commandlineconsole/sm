package uk.gov..mi.dw.servicedelivery

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import scala.collection.mutable.Map

import org.slf4j.LoggerFactory

import uk.gov..mi.dw.{DbConfig, DbHelper, MISrvcDlvryHubVaultConfig, MISrvcDlvrySatVaultConfig}
import uk.gov..mi.model._


class SrvcDlvryBaseVaultJob(batchId: String, timestampStr: String, spark: SparkSession) {

  val log = LoggerFactory.getLogger(SrvcDlvryBaseVaultJob.this.getClass)

  /**
   * Define the Optional Column Maps for the Satellite tables
   */
  val chlgOptColMap = Map(("src_created_by", ""), ("src_created_datetime", ""),("src_last_updated_by", "") ,
                          ("src_last_updated_datetime", ""),("src_created2_datetime", ""))

  val visOptColMap = Map(("srvc_dlvry_handle_visibility",""))

  val ehOptColMap = Map(("external_handle_value", ""), ("external_handle_space", ""))

  val mainOptColMap = Map(("srvc_dlvry_type_cd", ""), ("due_date", ""), ("priority", ""), ("primary_ref_type_cd", ""), ("primary_ref_value", ""))

  val attrOptColMap = Map(("attr_handle_visibility", ""), ("attr_type_cd", ""), ("attr_value_datetime", ""),
                          ("attr_value_string", ""), ("attr_value_number", ""),("attr_ref_data_cd", ""),
                          ("attr_created_by", ""), ("attr_created_datetime", ""))

  val procOptColMap = Map(("process_handle_visibility" , ""), ("process_created_by", ""), ("process_created_datetime", ""),
                          ("process_last_updated_by", ""), ("process_last_updated_datetime", ""), ("process_created2_datetime", ""))

  val instOptColMap = Map(("process_inst_start_datetime", "") , ("process_inst_end_datetime",""))

  def addMissingFieldsToDF(inputDF: DataFrame, colMap: Map[String, String]): DataFrame = {
    val colList = inputDF.columns
    var outputDF : DataFrame = inputDF
    colMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    outputDF
  }


  /**
    * Start()
    */
  def start(dbConfig: DbConfig,
            srvcDlvryHubVaultConfig: MISrvcDlvryHubVaultConfig,
            srvcDlvrySatVaultConfig: MISrvcDlvrySatVaultConfig): Unit =
  {

    import spark.implicits._       // The import is required for using $ prefix for Column Names

    /** [Step 0 - Initialize Dataframes to Empty Datasets of the correct Case Class Types */
    var DF0_RawInput_HubSrvcDlvry = spark.emptyDataset[HubServiceDelivery].toDF
    var DF0_RawInput_SatSrvcDlvryInfo = spark.emptyDataset[SServiceDeliveryInfo].toDF
    var DF0_RawInput_SatSrvcDlvryAttr = spark.emptyDataset[SServiceDeliveryAttribute].toDF
    var DF0_RawInput_SatSrvcDlvryProc = spark.emptyDataset[SServiceDeliveryProcess].toDF
    var DF0_RawInput_SatSrvcDlvryProcInst = spark.emptyDataset[SServiceDeliveryProcessInstance].toDF
    var DF0_RawInput_LnkSrvcDlvryParent = spark.emptyDataset[LinkServiceDeliveryParent].toDF


    /** [Step 1 - READ_INPUTS]
     *  - Read the input data files for Hub and Individual Satellites {FileSatA -> DF_A}, {FileSatB -> DF_B}
     */

    log.info("[SrvcDlvryBaseVaultJob] Reading the input data files for Service Delivery")

    // (1.1) Read Hub File

    try { DF0_RawInput_HubSrvcDlvry = spark.read.json(srvcDlvryHubVaultConfig.hubSrvcDlvryCoreInputPath)
    } catch {
       case ex: AnalysisException => DF0_RawInput_HubSrvcDlvry = spark.emptyDataset[HubServiceDelivery].toDF
    }

    // (1.2) Read Individual Satellite Files

    // A) Read Satellite Service Delivery Info
    try { DF0_RawInput_SatSrvcDlvryInfo = spark.read.json(srvcDlvrySatVaultConfig.satSrvcDlvryInfoInputPath)
    } catch {
        case ex: AnalysisException => DF0_RawInput_SatSrvcDlvryInfo = spark.emptyDataset[SServiceDeliveryInfo].toDF
    }

    // B) Read Satellite Service Delivery Attribute
    try { DF0_RawInput_SatSrvcDlvryAttr = spark.read.json(srvcDlvrySatVaultConfig.satSrvcDlvryAttributeInputPath)
    } catch {
        case ex: AnalysisException => DF0_RawInput_SatSrvcDlvryAttr = spark.emptyDataset[SServiceDeliveryAttribute].toDF
    }

    // C) Read Satellite Service Delivery Process
    try { DF0_RawInput_SatSrvcDlvryProc = spark.read.json(srvcDlvrySatVaultConfig.satSrvcDlvryProcessInputPath)
    } catch {
        case ex: AnalysisException => DF0_RawInput_SatSrvcDlvryProc = spark.emptyDataset[SServiceDeliveryProcess].toDF
    }

    // D) Read Satellite Service Delivery Process Instance
    try { DF0_RawInput_SatSrvcDlvryProcInst = spark.read.json(srvcDlvrySatVaultConfig.satSrvcDlvryProcessInstanceInputPath)
    } catch {
        case ex: AnalysisException => DF0_RawInput_SatSrvcDlvryProcInst = spark.emptyDataset[SServiceDeliveryProcessInstance].toDF
    }

    // E) Read Satellite Service Delivery Parent Link
    try { DF0_RawInput_LnkSrvcDlvryParent = spark.read.json(srvcDlvrySatVaultConfig.lnkSrvcDlvryParentInputPath)
    } catch {
        case ex: AnalysisException => DF0_RawInput_LnkSrvcDlvryParent = spark.emptyDataset[LinkServiceDeliveryParent].toDF
    }

    // Persist the Input DataFrames as these are used multiple times
    DF0_RawInput_HubSrvcDlvry.persist
    DF0_RawInput_SatSrvcDlvryInfo.persist
    DF0_RawInput_SatSrvcDlvryAttr.persist
    DF0_RawInput_SatSrvcDlvryProc.persist
    DF0_RawInput_SatSrvcDlvryProcInst.persist
    DF0_RawInput_LnkSrvcDlvryParent.persist

    /** [Step 2 - DEDUP_BSNKEY]
     *  - Get the set of Distinct Business Keys across the Files : (DF_HUB, DF_SAT_A + DF_SAT_B...) -> Distinct -> DF_BK
     */

    val DF0_Distinct_MessageId_SrvcDlvry = DF0_RawInput_HubSrvcDlvry.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime")
        .union(DF0_RawInput_SatSrvcDlvryInfo.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime"))
            .union(DF0_RawInput_SatSrvcDlvryAttr.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime"))
            .union(DF0_RawInput_SatSrvcDlvryProc.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime"))
            .union(DF0_RawInput_SatSrvcDlvryProcInst.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime"))
            .union(DF0_RawInput_LnkSrvcDlvryParent.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime"))
            .union(DF0_RawInput_LnkSrvcDlvryParent.select("message_id", "record_source","parent_srvc_dlvry_handle_id", "parent_srvc_dlvry_hk", "bsn_event_datetime"))
      .groupBy($"message_id", $"record_source", $"srvc_dlvry_handle_id")
          .agg(("bsn_event_datetime","min"), ("srvc_dlvry_hk", "min"))
      .withColumnRenamed("min(bsn_event_datetime)","msg_bsn_event_datetime")
          .withColumnRenamed("min(srvc_dlvry_hk)","srvc_dlvry_hk")
      .withColumn("msg_bsn_event_datetime", $"msg_bsn_event_datetime".cast("timestamp"))
      .select("message_id", "record_source", "srvc_dlvry_handle_id","srvc_dlvry_hk","msg_bsn_event_datetime")

    DF0_Distinct_MessageId_SrvcDlvry.persist

/*
  val DF0_Distinct_MessageId_SrvcDlvry = DF0_RawInput_HubSrvcDlvry.select("message_id", "record_source","srvc_dlvry_handle_id", "srvc_dlvry_hk", "bsn_event_datetime")
  .distinct()
*/
    val DF1_Distinct_BsnKey_SrvcDlvry = DF0_Distinct_MessageId_SrvcDlvry
                                            .groupBy($"record_source", $"srvc_dlvry_handle_id")
                                                .agg(("msg_bsn_event_datetime","min"),("srvc_dlvry_hk", "min") )
                                            .withColumnRenamed("min(msg_bsn_event_datetime)","bsn_event_datetime")
                                                .withColumnRenamed("min(srvc_dlvry_hk)","srvc_dlvry_hk")
                                            .select("record_source", "srvc_dlvry_handle_id","srvc_dlvry_hk","bsn_event_datetime")

    /** [Step 3 - RESOLVE_BSNKEY_HASH]
     * - Resolve the Hash Keys for the Business Keys (DF_BK -> DB -> View -> DF_HK)
     */

    DbHelper.clearDbTempTables(dbConfig = dbConfig
                              ,tabName  = "mi_dwb.stg_srvc_dlvry_bsn_key")

    DbHelper.writeDF( dbConfig = dbConfig
                    , tabName  = "mi_dwb.stg_srvc_dlvry_bsn_key"
                    , df       = DF1_Distinct_BsnKey_SrvcDlvry)

    val sqlQry_Resolve_N_Get_HK_for_HubSrvcDlvry = """(select
                                                           record_source          , srvc_dlvry_handle_id    , effective_datetime
                                                         , original_srvc_dlvry_hk , resolved_srvc_dlvry_hk  , updated_counter_value
                                                         , data_domain            , insert_flag
                                                       from
                                                           mi_dwb.vw_resolve_srvc_dlvry_hk
                                                      ) as subset"""

    val DF2_Distinct_HK_SrvcDlvry  = DbHelper.databaseDF( dbConfig = dbConfig
                                                        , sqlQuery = sqlQry_Resolve_N_Get_HK_for_HubSrvcDlvry  )(spark)

    DF2_Distinct_HK_SrvcDlvry.persist;  // Force the evaluation of DF2 and persist as this is required for DF3 and DF4


    /** [Step 4 - WRITE_HUB_RECS_TO_DB] : Store the resolved Hash Key values in Database (DF2(Insert=Y) -> DF3 -> DB) */
    val DF5_LoadReadySet_HubSrvcDlvry = DF2_Distinct_HK_SrvcDlvry.filter("insert_flag='Y'")
      .withColumnRenamed("resolved_srvc_dlvry_hk","srvc_dlvry_hk")
      .withColumn("dv_created_by_batch_id", lit(batchId))
          .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//          .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
      .select("srvc_dlvry_hk", "effective_datetime", "record_source",
        "srvc_dlvry_handle_id", "dv_created_by_batch_id", "dv_created_datetime")

    if (!DF5_LoadReadySet_HubSrvcDlvry.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table hub_srvc_dlvry")

      DbHelper.writeDF(dbConfig = dbConfig
        , tabName = "mi_dwb.hub_srvc_dlvry"
        , df = DF5_LoadReadySet_HubSrvcDlvry)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table hub_srvc_dlvry")
    }


    /** [Step 5 - UPD_KEY_HASH_COLLISION_COUNTER] : Update Collision Counter for any colliding records */
    val DF5_LoadReadySet_HKCollisionCounterSrvcDlvry = DF2_Distinct_HK_SrvcDlvry.filter($"insert_flag"==="Y")
      .groupBy("data_domain", "original_srvc_dlvry_hk")
      .agg(max("updated_counter_value"))
      .withColumnRenamed("original_srvc_dlvry_hk", "hash_key")
          .withColumnRenamed("max(updated_counter_value)", "counter_value")
      .filter($"counter_value" > 1)
      .select("data_domain", "hash_key", "counter_value")

    if (!DF5_LoadReadySet_HKCollisionCounterSrvcDlvry .rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table dv_hash_key_collision_counter")

      DbHelper.writeDF(dbConfig = dbConfig
        , tabName = "mi_dwb.dv_hash_key_collision_counter"
        , df = DF5_LoadReadySet_HKCollisionCounterSrvcDlvry)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table dv_hash_key_collision_counter")
    }

    /**
     * [Step 4 - GET_PRV_REC_HASH] : Get the Record Hash Value for the Business Keys from PIT (DB_STG_BK -> VW_GET_PIT -> DF_PIT)
     */

    val sqlQry_Get_Exs_PIT_for_SatSrvcDlvry =
        """(select
                   record_source             , srvc_dlvry_handle_id , message_id
                 , srvc_dlvry_hk             , effective_datetime
                 , sd_vsblty_eff_dtime       , sd_vsblty_rec_hash_value
                 , sd_ext_hndl_eff_dtime     , sd_ext_hndl_rec_hash_value
                 , sd_main_eff_dtime         , sd_main_rec_hash_value
                 , sd_status_eff_dtime       , sd_status_rec_hash_value
                 , sd_attr_list_eff_dtime    , sd_attr_list_agg_hash
                 , sd_process_list_eff_dtime , sd_process_list_agg_hash
                 , sd_process_inst_list_eff_dtime , sd_process_inst_list_agg_hash
                 , latest_rec_flag
            from
                 mi_dwb.vw_get_srvc_dlvry_pit
            ) as subset"""

    val DF3_Exs_PIT_with_HK_SatSrvcDlvry  = DbHelper.databaseDF( dbConfig = dbConfig
                                                               , sqlQuery = sqlQry_Get_Exs_PIT_for_SatSrvcDlvry )(spark)

    /**
     * [Step 5 - APPEND_HK_TO_INPUT] : Append HK to the Input Raw Data
     */

    // A) Append HK to the Input Satellite Service Delivery Info
    val DF3_InputWithHK_SatSrvcDlvryInfo = DF0_RawInput_SatSrvcDlvryInfo.as('sat)
                                                .join(DF2_Distinct_HK_SrvcDlvry.as('lkp_hk),
                                                          $"lkp_hk.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_hk.record_source" === $"sat.record_source")
                                                .join(DF0_Distinct_MessageId_SrvcDlvry.as('lkp_msg),
                                                          $"lkp_msg.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_msg.record_source" === $"sat.record_source"
                                                       && $"lkp_msg.message_id" === $"sat.message_id")
                                                .withColumnRenamed("resolved_srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                                .select("final_srvc_dlvry_hk","msg_bsn_event_datetime", "sat.*")

    // B) Append HK to the Input Satellite Service Delivery Attribute
    val DF3_InputWithHK_SatSrvcDlvryAttr = DF0_RawInput_SatSrvcDlvryAttr.as('sat)
                                                .join(DF2_Distinct_HK_SrvcDlvry.as('lkp_hk),
                                                          $"lkp_hk.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_hk.record_source" === $"sat.record_source")
                                                .join(DF0_Distinct_MessageId_SrvcDlvry.as('lkp_msg),
                                                          $"lkp_msg.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_msg.record_source" === $"sat.record_source"
                                                       && $"lkp_msg.message_id" === $"sat.message_id")
                                                .withColumnRenamed("resolved_srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                                .select("final_srvc_dlvry_hk","msg_bsn_event_datetime", "sat.*")

    // C) Append HK to the Input Satellite Service Delivery Process
    val DF3_InputWithHK_SatSrvcDlvryProc = DF0_RawInput_SatSrvcDlvryProc.as('sat)
                                                .join(DF2_Distinct_HK_SrvcDlvry.as('lkp_hk),
                                                          $"lkp_hk.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_hk.record_source" === $"sat.record_source")
                                                .join(DF0_Distinct_MessageId_SrvcDlvry.as('lkp_msg),
                                                          $"lkp_msg.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_msg.record_source" === $"sat.record_source"
                                                       && $"lkp_msg.message_id" === $"sat.message_id")
                                                .withColumnRenamed("resolved_srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                                .select("final_srvc_dlvry_hk","msg_bsn_event_datetime", "sat.*")

    // D) Append HK to the Input Satellite Service Delivery Process Instance
    val DF3_InputWithHK_SatSrvcDlvryProcInst = DF0_RawInput_SatSrvcDlvryProcInst.as('sat)
                                                .join(DF2_Distinct_HK_SrvcDlvry.as('lkp_hk),
                                                          $"lkp_hk.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_hk.record_source" === $"sat.record_source")
                                                .join(DF0_Distinct_MessageId_SrvcDlvry.as('lkp_msg),
                                                          $"lkp_msg.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_msg.record_source" === $"sat.record_source"
                                                       && $"lkp_msg.message_id" === $"sat.message_id")
                                                .withColumnRenamed("resolved_srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                                .select("final_srvc_dlvry_hk","msg_bsn_event_datetime", "sat.*")

    // E) Append HK to the Input Link Service Delivery Parent
    val DF3_InputWithHK_LnkSrvcDlvryParent = DF0_RawInput_LnkSrvcDlvryParent.as('sat)
                                                .join(DF2_Distinct_HK_SrvcDlvry.as('lkp_hk),
                                                          $"lkp_hk.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_hk.record_source" === $"sat.record_source")
                                                .join(DF0_Distinct_MessageId_SrvcDlvry.as('lkp_msg),
                                                          $"lkp_msg.srvc_dlvry_handle_id" === $"sat.srvc_dlvry_handle_id"
                                                       && $"lkp_msg.record_source" === $"sat.record_source"
                                                       && $"lkp_msg.message_id" === $"sat.message_id")
                                                .withColumnRenamed("resolved_srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                                .select("final_srvc_dlvry_hk","msg_bsn_event_datetime", "sat.*")


    /** [Step 6 - COMBINE_EXS_N_CURR_RECS]
      * Combine the Existing Latest Rec Hash Value with New Records' Rec Hash Value
      * (DF_ALL(A) = DF_PIT(A) + DF_A; DF_ALL(B) = DF_PIT(B) + DF_B;... )
      */

    // A) Satellite Service Delivery Change Log

    val DF5_LoadReadySet_SatSrvcDlvryChlg = addMissingFieldsToDF(DF3_InputWithHK_SatSrvcDlvryInfo.distinct(), chlgOptColMap)
          .select("final_srvc_dlvry_hk", "msg_bsn_event_datetime", "record_source",
                  "chlg_rec_hash_value",
                  "message_id", "src_created_by", "src_created_datetime",
                  "src_last_updated_by","src_last_updated_datetime", "src_created2_datetime")
          .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
              .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
              .withColumnRenamed("chlg_rec_hash_value", "rec_hash_value")
              .withColumnRenamed("message_id", "src_message_id")
          .withColumn("dv_created_by_batch_id", lit(batchId))
              .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
  //            .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
              .withColumn("src_created_datetime", $"src_created_datetime".cast("timestamp"))
              .withColumn("src_last_updated_datetime", $"src_last_updated_datetime".cast("timestamp"))
              .withColumn("src_created2_datetime", $"src_created2_datetime".cast("timestamp"))
          .select("srvc_dlvry_hk", "effective_datetime", "record_source",
                  "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
                  "src_message_id", "src_created_by", "src_created_datetime",
                  "src_last_updated_by","src_last_updated_datetime", "src_created2_datetime")

    if (!DF5_LoadReadySet_SatSrvcDlvryChlg.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_change_log")

      DbHelper.writeDF( dbConfig = dbConfig
        , tabName  = "mi_dwb.s_srvc_dlvry_change_log"
        , df       = DF5_LoadReadySet_SatSrvcDlvryChlg)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_change_log")
    }
    // B) Satellite Service Delivery Visibility

    val DF4_New_SatSrvcDlvryVsblty =  addMissingFieldsToDF(DF3_InputWithHK_SatSrvcDlvryInfo.distinct(), visOptColMap)
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("msg_bsn_event_datetime", $"msg_bsn_event_datetime".cast("timestamp"))
                                          .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "vis_rec_hash_value","srvc_dlvry_handle_visibility", "data_status")

    val DF4_Exs_SatSrvcDlvryVsblty = DF3_Exs_PIT_with_HK_SatSrvcDlvry
//                                         .filter($"latest_rec_flag" === "Y")
                                         .select("message_id", "srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id",
                                                 "sd_vsblty_rec_hash_value", "effective_datetime")
                                         .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                             .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                             .withColumnRenamed("sd_vsblty_rec_hash_value", "vis_rec_hash_value")
                                         .withColumn("srvc_dlvry_handle_visibility", lit(null).cast(StringType))
                                             .withColumn("data_status", lit("E"))
                                         .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                 "vis_rec_hash_value","srvc_dlvry_handle_visibility", "data_status")

    val DF4_ExsUnionNew_SatSrvcDlvryVsblty = DF4_New_SatSrvcDlvryVsblty.union(DF4_Exs_SatSrvcDlvryVsblty)

    DF4_ExsUnionNew_SatSrvcDlvryVsblty.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryVsblty")

    val sqlQry_Compress_SatSrvcDlvryVsblty =
      """select
                ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.vis_rec_hash_value           as vis_rec_hash_value
              , ilv.srvc_dlvry_handle_visibility as srvc_dlvry_handle_visibility
         from
              (select
                      final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , vis_rec_hash_value
                    , srvc_dlvry_handle_visibility
                    , data_status
                    , lag(vis_rec_hash_value,1) over (partition by final_srvc_dlvry_hk
                                                      order by msg_bsn_event_datetime, message_id, data_status) prev_vis_rec_hash_value
              from vw_ExsNew_SatSrvcDlvryVsblty
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_vis_rec_hash_value is null
              or ilv.vis_rec_hash_value != ilv.prev_vis_rec_hash_value)""".stripMargin

    val DF5_LoadReadySet_SatSrvcDlvryVsblty = spark.sql(sqlQry_Compress_SatSrvcDlvryVsblty)
      .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
          .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
          .withColumnRenamed("vis_rec_hash_value", "rec_hash_value")
      .withColumn("dv_created_by_batch_id", lit(batchId))
          .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//              .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
      .select("srvc_dlvry_hk", "effective_datetime", "record_source",
              "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "srvc_dlvry_handle_visibility")

    if (!DF5_LoadReadySet_SatSrvcDlvryVsblty.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_visibility")

      DbHelper.writeDF( dbConfig = dbConfig
                      , tabName  = "mi_dwb.s_srvc_dlvry_visibility"
                      , df       = DF5_LoadReadySet_SatSrvcDlvryVsblty)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_visibility")
    }


    // C) Satellite Service Delivery External Handle

    val DF4_New_SatSrvcDlvryExtHndl  =  addMissingFieldsToDF(DF3_InputWithHK_SatSrvcDlvryInfo.distinct(), ehOptColMap)
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))
                                          .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "eh_rec_hash_value","external_handle_value", "external_handle_space",
                                                  "data_status")

    val DF4_Exs_SatSrvcDlvryExtHndl = DF3_Exs_PIT_with_HK_SatSrvcDlvry
//                                         .filter($"latest_rec_flag" === "Y")
                                         .select("message_id", "srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id",
                                                 "sd_ext_hndl_rec_hash_value", "effective_datetime")
                                         .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                             .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                             .withColumnRenamed("sd_ext_hndl_rec_hash_value", "eh_rec_hash_value")
                                         .withColumn("external_handle_value", lit(null).cast(StringType))
                                             .withColumn("external_handle_space", lit(null).cast(StringType))
                                             .withColumn("data_status", lit("E"))
                                         .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                 "eh_rec_hash_value","external_handle_value", "external_handle_space",
                                                 "data_status")

    val DF4_ExsUnionNew_SatSrvcDlvryExtHndl = DF4_New_SatSrvcDlvryExtHndl.union(DF4_Exs_SatSrvcDlvryExtHndl)

    DF4_ExsUnionNew_SatSrvcDlvryExtHndl.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryExtHndl")

    val sqlQry_Compress_SatSrvcDlvryExtHndl =
      """select
                ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.eh_rec_hash_value            as eh_rec_hash_value
              , ilv.external_handle_value        as external_handle_value
              , ilv.external_handle_space        as external_handle_space
         from
              (select
                      final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , eh_rec_hash_value
                    , external_handle_value
                    , external_handle_space
                    , data_status
                    , lag(eh_rec_hash_value,1) over (partition by final_srvc_dlvry_hk
                                                      order by msg_bsn_event_datetime, message_id, data_status) prev_eh_rec_hash_value
              from vw_ExsNew_SatSrvcDlvryExtHndl
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_eh_rec_hash_value is null
              or ilv.eh_rec_hash_value != ilv.prev_eh_rec_hash_value)""".stripMargin

    val DF5_LoadReadySet_SatSrvcDlvryExtHndl = spark.sql(sqlQry_Compress_SatSrvcDlvryExtHndl)
      .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
          .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
          .withColumnRenamed("eh_rec_hash_value", "rec_hash_value")
      .withColumn("dv_created_by_batch_id", lit(batchId))
          .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//          .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
      .select("srvc_dlvry_hk", "effective_datetime", "record_source",
              "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
              "external_handle_value", "external_handle_space")

    if (!DF5_LoadReadySet_SatSrvcDlvryExtHndl.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_external_handle")

      DbHelper.writeDF( dbConfig = dbConfig
                      , tabName  = "mi_dwb.s_srvc_dlvry_external_handle"
                      , df       = DF5_LoadReadySet_SatSrvcDlvryExtHndl)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_external_handle")
    }


    // D) Satellite Service Delivery Main

    val DF4_New_SatSrvcDlvryMain  =  addMissingFieldsToDF(DF3_InputWithHK_SatSrvcDlvryInfo.distinct(), mainOptColMap)
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))
                                          .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "main_rec_hash_value","srvc_dlvry_type_cd", "due_date",
                                                  "priority", "primary_ref_type_cd", "primary_ref_value",
                                                  "data_status")

    val DF4_Exs_SatSrvcDlvryMain = DF3_Exs_PIT_with_HK_SatSrvcDlvry
//                                         .filter($"latest_rec_flag" === "Y")
                                         .select("message_id", "srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id",
                                                 "sd_main_rec_hash_value", "effective_datetime")
                                         .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                             .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                             .withColumnRenamed("sd_main_rec_hash_value", "main_rec_hash_value")
                                         .withColumn("srvc_dlvry_type_cd", lit(null).cast(StringType))
                                             .withColumn("due_date", lit(null).cast(StringType))
                                             .withColumn("priority", lit(null).cast(StringType))
                                             .withColumn("primary_ref_type_cd", lit(null).cast(StringType))
                                             .withColumn("primary_ref_value", lit(null).cast(StringType))
                                             .withColumn("data_status", lit("E"))
                                         .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                 "main_rec_hash_value","srvc_dlvry_type_cd", "due_date",
                                                 "priority", "primary_ref_type_cd", "primary_ref_value",
                                                 "data_status")


    val DF4_ExsUnionNew_SatSrvcDlvryMain = DF4_New_SatSrvcDlvryMain.union(DF4_Exs_SatSrvcDlvryMain)

    DF4_ExsUnionNew_SatSrvcDlvryMain.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryMain")

    val sqlQry_Compress_SatSrvcDlvryMain =
      """select
                ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.main_rec_hash_value          as main_rec_hash_value
              , ilv.srvc_dlvry_type_cd           as srvc_dlvry_type_cd
              , ilv.due_date                     as due_date
              , ilv.priority                     as priority
              , ilv.primary_ref_type_cd          as primary_ref_type_cd
              , ilv.primary_ref_value            as primary_ref_value
          from
              (select
                      final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , main_rec_hash_value
                    , srvc_dlvry_type_cd
                    , due_date
                    , priority
                    , primary_ref_type_cd
                    , primary_ref_value
                    , data_status
                    , lag(main_rec_hash_value,1) over (partition by final_srvc_dlvry_hk
                                                      order by msg_bsn_event_datetime, message_id, data_status) prev_main_rec_hash_value
              from vw_ExsNew_SatSrvcDlvryMain
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_main_rec_hash_value is null
              or ilv.main_rec_hash_value != ilv.prev_main_rec_hash_value)""".stripMargin

    val DF5_LoadReadySet_SatSrvcDlvryMain = spark.sql(sqlQry_Compress_SatSrvcDlvryMain)
      .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
          .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
          .withColumnRenamed("main_rec_hash_value", "rec_hash_value")
      .withColumn("dv_created_by_batch_id", lit(batchId))
          .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//          .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
          .withColumn("due_date", $"due_date".cast("timestamp"))
          .withColumn("priority", $"priority".cast("integer"))
      .select("srvc_dlvry_hk", "effective_datetime", "record_source",
              "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
              "srvc_dlvry_type_cd", "due_date", "priority", "primary_ref_type_cd", "primary_ref_value")

    if (!DF5_LoadReadySet_SatSrvcDlvryMain.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_main")

      DbHelper.writeDF( dbConfig = dbConfig
                      , tabName  = "mi_dwb.s_srvc_dlvry_main"
                      , df       = DF5_LoadReadySet_SatSrvcDlvryMain)
    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_main")
    }
    // E) Satellite Service Delivery Status

    val DF4_New_SatSrvcDlvryStatus  =  DF3_InputWithHK_SatSrvcDlvryInfo
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))
                                          .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "status_rec_hash_value","srvc_dlvry_status_cd",
                                                  "data_status")

    val DF4_Exs_SatSrvcDlvryStatus = DF3_Exs_PIT_with_HK_SatSrvcDlvry
//                                         .filter($"latest_rec_flag" === "Y")
                                         .select("message_id", "srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id",
                                                 "sd_status_rec_hash_value", "effective_datetime")
                                         .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                             .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                             .withColumnRenamed("sd_status_rec_hash_value", "status_rec_hash_value")
                                         .withColumn("srvc_dlvry_status_cd", lit(null).cast(StringType))
                                             .withColumn("data_status", lit("E"))
                                          .select("message_id", "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "status_rec_hash_value","srvc_dlvry_status_cd",
                                                  "data_status")

    val DF4_ExsUnionNew_SatSrvcDlvryStatus = DF4_New_SatSrvcDlvryStatus.union(DF4_Exs_SatSrvcDlvryStatus)

    DF4_ExsUnionNew_SatSrvcDlvryStatus.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryStatus")

    val sqlQry_Compress_SatSrvcDlvryStatus =
      """select
                ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.status_rec_hash_value        as status_rec_hash_value
              , ilv.srvc_dlvry_status_cd         as srvc_dlvry_status_cd
          from
              (select
                      final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , status_rec_hash_value
                    , srvc_dlvry_status_cd
                    , data_status
                    , lag(status_rec_hash_value,1) over (partition by final_srvc_dlvry_hk
                                                      order by msg_bsn_event_datetime, message_id, data_status) prev_status_rec_hash_value
              from vw_ExsNew_SatSrvcDlvryStatus
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_status_rec_hash_value is null
              or ilv.status_rec_hash_value != ilv.prev_status_rec_hash_value)""".stripMargin

    val DF5_LoadReadySet_SatSrvcDlvryStatus = spark.sql(sqlQry_Compress_SatSrvcDlvryStatus)
      .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
          .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
          .withColumnRenamed("status_rec_hash_value", "rec_hash_value")
      .withColumn("dv_created_by_batch_id", lit(batchId))
          .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//          .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
      .select("srvc_dlvry_hk", "effective_datetime", "record_source",
              "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
              "srvc_dlvry_status_cd")

    if (!DF5_LoadReadySet_SatSrvcDlvryStatus.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_status")

      DbHelper.writeDF( dbConfig = dbConfig
                      , tabName  = "mi_dwb.s_srvc_dlvry_status"
                      , df       = DF5_LoadReadySet_SatSrvcDlvryStatus)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_status")
    }

    // List Satellites

    // A) List Satellite Service Delivery Attribute
    val DF4_New_SatSrvcDlvryAttr  =  DF3_InputWithHK_SatSrvcDlvryAttr
                                          .filter($"rec_seqno" === "0")
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))
                                          .select("message_id",
                                                  "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "attr_list_agg_hash",
                                                  "data_status")

    val DF4_Exs_SatSrvcDlvryAttr = DF3_Exs_PIT_with_HK_SatSrvcDlvry
//                                          .filter($"latest_rec_flag" === "Y")
                                          .select("srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id","message_id",
                                                  "sd_attr_list_agg_hash", "effective_datetime")
                                          .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                              .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                              .withColumnRenamed("sd_attr_list_agg_hash", "attr_list_agg_hash")
                                          .withColumn("data_status", lit("E"))
//                                              .withColumn("message_id", lit(null).cast(StringType))
                                          .select("message_id",
                                                  "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "attr_list_agg_hash",
                                                  "data_status")

    val DF4_ExsUnionNew_SatSrvcDlvryAttr = DF4_New_SatSrvcDlvryAttr.union(DF4_Exs_SatSrvcDlvryAttr)

    DF4_ExsUnionNew_SatSrvcDlvryAttr.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryAttr")

    val sqlQry_Compress_SatSrvcDlvryAttr =
      """select
                ilv.message_id                   as message_id
              , ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.attr_list_agg_hash           as attr_list_agg_hash
          from
              (select
                      message_id
                    , final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , attr_list_agg_hash
                    , data_status
                    , lag(attr_list_agg_hash,1) over (partition by final_srvc_dlvry_hk
                                                      order by msg_bsn_event_datetime, message_id, data_status) prev_attr_list_agg_hash
              from vw_ExsNew_SatSrvcDlvryAttr
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_attr_list_agg_hash is null
              or ilv.attr_list_agg_hash != ilv.prev_attr_list_agg_hash)""".stripMargin

    val DF5_LoadReadyKeys_SatSrvcDlvryAttr = spark.sql(sqlQry_Compress_SatSrvcDlvryAttr)

    val DF5_LoadReadyData_SatSrvcDlvryAttr = addMissingFieldsToDF(DF3_InputWithHK_SatSrvcDlvryAttr, attrOptColMap).as('inp)
                                                .join(DF5_LoadReadyKeys_SatSrvcDlvryAttr.as('lrd_key),
                                                          $"inp.message_id" === $"lrd_key.message_id"
                                                       && $"inp.final_srvc_dlvry_hk" === $"lrd_key.final_srvc_dlvry_hk"
                                                       && $"inp.msg_bsn_event_datetime" === $"lrd_key.msg_bsn_event_datetime")
                                                .select("inp.*")
                                                  .drop("srvc_dlvry_hk")
                                                .distinct()
                                                .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
                                                    .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
                                                .withColumn("dv_created_by_batch_id", lit(batchId))
                                                    .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//                                                    .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
                                                    .withColumn("attr_value_datetime", $"attr_value_datetime".cast("timestamp"))
                                                    .withColumn("attr_created_datetime", $"attr_created_datetime".cast("timestamp"))
                                                    .withColumn("attr_value_number", $"attr_value_number".cast("float"))
                                                    .withColumn("rec_seqno", $"rec_seqno".cast("integer"))
                                                .select("srvc_dlvry_hk", "attr_handle_id", "effective_datetime", "record_source",
                                                        "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
                                                        "attr_handle_visibility", "attr_type_cd", "attr_value_datetime",
                                                        "attr_value_string","attr_value_number","attr_ref_data_cd",
                                                        "attr_created_by","attr_created_datetime", "attr_list_agg_hash", "rec_seqno" )


    if (!DF5_LoadReadyData_SatSrvcDlvryAttr.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_attribute")

      DbHelper.writeDF( dbConfig = dbConfig
        , tabName  = "mi_dwb.s_srvc_dlvry_attribute"
        , df       = DF5_LoadReadyData_SatSrvcDlvryAttr)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_attribute")
    }

    // B) List Satellite Service Delivery Process
    val DF4_New_SatSrvcDlvryProc  =  DF3_InputWithHK_SatSrvcDlvryProc
                                          .filter($"rec_seqno" === "0")
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))
                                          .select("message_id",
                                                  "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "process_list_agg_hash",
                                                  "data_status")

    val DF4_Exs_SatSrvcDlvryProc = DF3_Exs_PIT_with_HK_SatSrvcDlvry
  //                                        .filter($"latest_rec_flag" === "Y")
                                          .select("srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id","message_id",
                                                  "sd_process_list_agg_hash", "effective_datetime")
                                          .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                              .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                              .withColumnRenamed("sd_process_list_agg_hash", "process_list_agg_hash")
                                          .withColumn("data_status", lit("E"))
//                                              .withColumn("message_id", lit(null).cast(StringType))
                                          .select("message_id",
                                                  "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "process_list_agg_hash",
                                                  "data_status")

    val DF4_ExsUnionNew_SatSrvcDlvryProc = DF4_New_SatSrvcDlvryProc.union(DF4_Exs_SatSrvcDlvryProc)

    DF4_ExsUnionNew_SatSrvcDlvryProc.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryProc")

    val sqlQry_Compress_SatSrvcDlvryProc =
      """select
                ilv.message_id                   as message_id
              , ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.process_list_agg_hash        as process_list_agg_hash
          from
              (select
                      message_id
                    , final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , process_list_agg_hash
                    , data_status
                    , lag(process_list_agg_hash,1) over (partition by final_srvc_dlvry_hk
                                                         order by msg_bsn_event_datetime, message_id, data_status) prev_process_list_agg_hash
              from vw_ExsNew_SatSrvcDlvryProc
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_process_list_agg_hash is null
              or ilv.process_list_agg_hash != ilv.prev_process_list_agg_hash)""".stripMargin

    val DF5_LoadReadyKeys_SatSrvcDlvryProc = spark.sql(sqlQry_Compress_SatSrvcDlvryProc)

    val DF5_LoadReadyData_SatSrvcDlvryProc = DF3_InputWithHK_SatSrvcDlvryProc.as('inp)
                                                .join(DF5_LoadReadyKeys_SatSrvcDlvryProc.as('lrd_key),
                                                          $"inp.message_id" === $"lrd_key.message_id"
                                                       && $"inp.final_srvc_dlvry_hk" === $"lrd_key.final_srvc_dlvry_hk"
                                                       && $"inp.msg_bsn_event_datetime" === $"lrd_key.msg_bsn_event_datetime")
                                                .select("inp.*")
                                                    .drop("srvc_dlvry_hk")
                                                .distinct()
                                                .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
                                                    .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
                                                .withColumn("dv_created_by_batch_id", lit(batchId))
                                                    .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//                                                    .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
                                                    .withColumn("process_created_datetime", $"process_created_datetime".cast("timestamp"))
                                                    .withColumn("process_last_updated_datetime", $"process_last_updated_datetime".cast("timestamp"))
                                                    .withColumn("process_created2_datetime", $"process_created2_datetime".cast("timestamp"))
                                                    .withColumn("rec_seqno", $"rec_seqno".cast("integer"))
                                                .select("srvc_dlvry_hk", "process_handle_id", "effective_datetime", "record_source",
                                                        "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
                                                        "process_handle_visibility", "srvc_dlvry_stage_cd", "process_status_cd",
                                                        "process_created_by","process_created_datetime","process_last_updated_by",
                                                        "process_last_updated_datetime","process_created2_datetime", "process_list_agg_hash", "rec_seqno" )


    if (!DF5_LoadReadyData_SatSrvcDlvryProc.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_process")

      DbHelper.writeDF( dbConfig = dbConfig
        , tabName  = "mi_dwb.s_srvc_dlvry_process"
        , df       = DF5_LoadReadyData_SatSrvcDlvryProc)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_process")
    }

    // C) List Satellite Service Delivery ProcessInstance
    val DF4_New_SatSrvcDlvryProcInst  =  DF3_InputWithHK_SatSrvcDlvryProcInst
                                          .filter($"rec_seqno" === "0")
                                          .withColumn("data_status", lit("N"))
//                                          .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))
                                          .select("message_id",
                                                  "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "process_inst_list_agg_hash",
                                                  "data_status")

    val DF4_Exs_SatSrvcDlvryProcInst = DF3_Exs_PIT_with_HK_SatSrvcDlvry
//                                          .filter($"latest_rec_flag" === "Y")
                                          .select("srvc_dlvry_hk", "record_source", "srvc_dlvry_handle_id", "message_id",
                                                  "sd_process_inst_list_agg_hash", "effective_datetime")
                                          .withColumnRenamed("effective_datetime", "msg_bsn_event_datetime")
                                              .withColumnRenamed("srvc_dlvry_hk", "final_srvc_dlvry_hk")
                                              .withColumnRenamed("sd_process_inst_list_agg_hash", "process_inst_list_agg_hash")
                                          .withColumn("data_status", lit("E"))
//                                              .withColumn("message_id", lit(null).cast(StringType))
                                          .select("message_id",
                                                  "final_srvc_dlvry_hk", "record_source","srvc_dlvry_handle_id", "msg_bsn_event_datetime",
                                                  "process_inst_list_agg_hash",
                                                  "data_status")

    val DF4_ExsUnionNew_SatSrvcDlvryProcInst = DF4_New_SatSrvcDlvryProcInst.union(DF4_Exs_SatSrvcDlvryProcInst)

    DF4_ExsUnionNew_SatSrvcDlvryProcInst.createOrReplaceTempView("vw_ExsNew_SatSrvcDlvryProcInst")

    val sqlQry_Compress_SatSrvcDlvryProcInst =
      """select
                ilv.message_id                   as message_id
              , ilv.final_srvc_dlvry_hk          as final_srvc_dlvry_hk
              , ilv.record_source                as record_source
              , ilv.srvc_dlvry_handle_id         as srvc_dlvry_handle_id
              , ilv.msg_bsn_event_datetime       as msg_bsn_event_datetime
              , ilv.process_inst_list_agg_hash   as process_inst_list_agg_hash
          from
              (select
                      message_id
                    , final_srvc_dlvry_hk
                    , record_source
                    , srvc_dlvry_handle_id
                    , msg_bsn_event_datetime
                    , process_inst_list_agg_hash
                    , data_status
                    , lag(process_inst_list_agg_hash,1) over (partition by final_srvc_dlvry_hk
                                                              order by msg_bsn_event_datetime, message_id, data_status) prev_proc_inst_list_agg_hash
              from vw_ExsNew_SatSrvcDlvryProcInst
              ) ilv
         where ilv.data_status = 'N'
         and (   ilv.prev_proc_inst_list_agg_hash is null
              or ilv.process_inst_list_agg_hash != ilv.prev_proc_inst_list_agg_hash)""".stripMargin

    val DF5_LoadReadyKeys_SatSrvcDlvryProcInst = spark.sql(sqlQry_Compress_SatSrvcDlvryProcInst)

    val DF5_LoadReadyData_SatSrvcDlvryProcInst = DF3_InputWithHK_SatSrvcDlvryProcInst.as('inp)
                                                .join(DF5_LoadReadyKeys_SatSrvcDlvryProcInst.as('lrd_key),
                                                          $"inp.message_id" === $"lrd_key.message_id"
                                                       && $"inp.final_srvc_dlvry_hk" === $"lrd_key.final_srvc_dlvry_hk"
                                                       && $"inp.msg_bsn_event_datetime" === $"lrd_key.msg_bsn_event_datetime")
                                                .select("inp.*" )
                                                    .drop("srvc_dlvry_hk")
                                                .distinct()
                                                .withColumnRenamed("msg_bsn_event_datetime", "effective_datetime")
                                                    .withColumnRenamed("final_srvc_dlvry_hk", "srvc_dlvry_hk")
                                                    .withColumnRenamed("process_inst_id", "process_inst_handle_id")
                                                .withColumn("dv_created_by_batch_id", lit(batchId))
                                                    .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
//                                                    .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
                                                    .withColumn("process_inst_start_datetime", $"process_inst_start_datetime".cast("timestamp"))
                                                    .withColumn("process_inst_end_datetime", $"process_inst_end_datetime".cast("timestamp"))
                                                    .withColumn("rec_seqno", $"rec_seqno".cast("integer"))
                                                .select("srvc_dlvry_hk", "process_inst_handle_id", "effective_datetime", "record_source",
                                                        "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value",
                                                        "process_id", "process_inst_status_cd", "process_inst_stage_cd",
                                                        "process_inst_start_datetime","process_inst_end_datetime",
                                                        "process_inst_list_agg_hash", "rec_seqno" )


    if (!DF5_LoadReadyData_SatSrvcDlvryProcInst.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_process_instance")

      DbHelper.writeDF( dbConfig = dbConfig
        , tabName  = "mi_dwb.s_srvc_dlvry_process_instance"
        , df       = DF5_LoadReadyData_SatSrvcDlvryProcInst)
    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_process_instance")
    }

    // Step 6(SatA) - COMPARE_PRV_N_CURR_REC_HASH : Compare the Previous Rec Hash Value with New Records' Rec Hash Value and derive Insert flag (DF_ALL(A) + Insert Flag (Y/N))

    // Step 6(SatB) - COMPARE_PRV_N_CURR_REC_HASH : Compare the Previous Rec Hash Value with New Records' Rec Hash Value and derive Insert flag (DF_ALL(A) + Insert Flag (Y/N))


    // Step 7(SatA) - WRITE_OUTPUT : Store the changed Satellite Records in Database (DF_ALL(A).filter(Insert=Y) -> DB_SAT)

    val DF3_Input_PitSrvcDlvryInfo = DF0_RawInput_SatSrvcDlvryInfo
            .select("message_id", "record_source", "srvc_dlvry_handle_id",
                    "vis_rec_hash_value", "eh_rec_hash_value", "main_rec_hash_value","status_rec_hash_value")
            .withColumnRenamed("vis_rec_hash_value","sd_vsblty_rec_hash_value")
                .withColumnRenamed("eh_rec_hash_value","sd_ext_hndl_rec_hash_value")
                .withColumnRenamed("main_rec_hash_value","sd_main_rec_hash_value")
                .withColumnRenamed("status_rec_hash_value","sd_status_rec_hash_value")
            .distinct()

    val DF3_Input_PitSrvcDlvryAttr = DF0_RawInput_SatSrvcDlvryAttr
            .filter($"rec_seqno" === "0")
            .select("message_id", "record_source", "srvc_dlvry_handle_id",
                    "attr_list_agg_hash")
            .withColumnRenamed("attr_list_agg_hash","sd_attr_list_agg_hash")
            .distinct()

    val DF3_Input_PitSrvcDlvryProc = DF0_RawInput_SatSrvcDlvryProc
            .filter($"rec_seqno" === "0")
            .select("message_id", "record_source", "srvc_dlvry_handle_id",
                    "process_list_agg_hash")
            .withColumnRenamed("process_list_agg_hash","sd_process_list_agg_hash")
            .distinct()

    val DF3_Input_PitSrvcDlvryProcInst = DF0_RawInput_SatSrvcDlvryProcInst
            .filter($"rec_seqno" === "0")
            .select("message_id", "record_source", "srvc_dlvry_handle_id",
                    "process_inst_list_agg_hash")
            .withColumnRenamed("process_inst_list_agg_hash","sd_process_inst_list_agg_hash")
            .distinct()

    val DF3_New_PIT_SrvcDlvry = DF0_Distinct_MessageId_SrvcDlvry.as('msg)
            .join(DF3_Input_PitSrvcDlvryInfo.as('info),   $"msg.message_id" === $"info.message_id"
                                                       && $"msg.record_source" === $"info.record_source"
                                                       && $"msg.srvc_dlvry_handle_id" === $"info.srvc_dlvry_handle_id","leftouter")
                .join(DF3_Input_PitSrvcDlvryAttr.as('attr),   $"msg.message_id" === $"attr.message_id"
                                                           && $"msg.record_source" === $"attr.record_source"
                                                           && $"msg.srvc_dlvry_handle_id" === $"attr.srvc_dlvry_handle_id","leftouter")
                .join(DF3_Input_PitSrvcDlvryProc.as('proc),   $"msg.message_id" === $"proc.message_id"
                                                           && $"msg.record_source" === $"proc.record_source"
                                                           && $"msg.srvc_dlvry_handle_id" === $"proc.srvc_dlvry_handle_id","leftouter")
                .join(DF3_Input_PitSrvcDlvryProcInst.as('inst),  $"msg.message_id" === $"inst.message_id"
                                                              && $"msg.record_source" === $"inst.record_source"
                                                              && $"msg.srvc_dlvry_handle_id" === $"inst.srvc_dlvry_handle_id","leftouter")
                .join(DF2_Distinct_HK_SrvcDlvry.as('lkp_hk),     $"msg.record_source" === $"lkp_hk.record_source"
                                                              && $"msg.srvc_dlvry_handle_id" === $"lkp_hk.srvc_dlvry_handle_id")
            .select("msg.message_id", "lkp_hk.resolved_srvc_dlvry_hk", "msg.msg_bsn_event_datetime",
                    "info.sd_vsblty_rec_hash_value", "info.sd_ext_hndl_rec_hash_value", "info.sd_main_rec_hash_value","info.sd_status_rec_hash_value",
                    "attr.sd_attr_list_agg_hash","proc.sd_process_list_agg_hash","inst.sd_process_inst_list_agg_hash")
            .withColumnRenamed("resolved_srvc_dlvry_hk", "srvc_dlvry_hk")
                .withColumnRenamed("msg_bsn_event_datetime", "bsn_event_datetime")
//            .withColumn("bsn_event_datetime", $"bsn_event_datetime".cast("timestamp"))

    if (!DF3_New_PIT_SrvcDlvry.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table stg_srvc_dlvry_pit")

      DbHelper.clearDbTempTables(dbConfig = dbConfig
                                ,tabName  = "mi_dwb.stg_srvc_dlvry_pit")

      DbHelper.writeDF( dbConfig = dbConfig
                      , tabName  = "mi_dwb.stg_srvc_dlvry_pit"
                      , df       = DF3_New_PIT_SrvcDlvry)

    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table stg_srvc_dlvry_pit")
    }

    val sqlQry_Resolve_PIT_for_SrvcDlvry = """(select
                                                      "srvc_dlvry_hk", "effective_datetime", "expiry_datetime", "message_id",
                                                      "sd_vsblty_eff_dtime", "sd_vsblty_rec_hash_value",
                                                      "sd_ext_hndl_eff_dtime", "sd_ext_hndl_rec_hash_value",
                                                      "sd_main_eff_dtime", "sd_main_rec_hash_value",
                                                      "sd_status_eff_dtime", "sd_status_rec_hash_value",
                                                      "sd_attr_list_eff_dtime", "sd_attr_list_agg_hash",
                                                      "sd_process_list_eff_dtime", "sd_process_list_agg_hash",
                                                      "sd_process_inst_list_eff_dtime", "sd_process_inst_list_agg_hash"
                                               from
                                                      mi_dwb.vw_resolve_srvc_dlvry_pit
                                                      ) as subset"""

    val DF4_Resolved_PITSrvcDlvry = DbHelper.databaseDF( dbConfig = dbConfig
                                                       , sqlQuery = sqlQry_Resolve_PIT_for_SrvcDlvry  )(spark)

    val DF5_LoadReadySet_PITSrvcDlvry = DF4_Resolved_PITSrvcDlvry
                                            .withColumn("dv_created_by_batch_id", lit(batchId))
                                                .withColumn("dv_created_datetime", lit(timestampStr).cast("timestamp"))
                                                .withColumn("dv_last_updated_by_batch_id", lit(batchId))
                                                .withColumn("dv_last_updated_datetime", lit(timestampStr).cast("timestamp"))
//                                                .withColumn("effective_datetime", $"effective_datetime".cast("timestamp"))
                                                .withColumn("expiry_datetime", $"expiry_datetime".cast("timestamp"))
                                                .withColumn("sd_vsblty_eff_dtime", $"sd_vsblty_eff_dtime".cast("timestamp"))
                                                .withColumn("sd_ext_hndl_eff_dtime", $"sd_ext_hndl_eff_dtime".cast("timestamp"))
                                                .withColumn("sd_main_eff_dtime", $"sd_main_eff_dtime".cast("timestamp"))
                                                .withColumn("sd_status_eff_dtime", $"sd_status_eff_dtime".cast("timestamp"))
                                                .withColumn("sd_attr_list_eff_dtime", $"sd_attr_list_eff_dtime".cast("timestamp"))
                                                .withColumn("sd_process_list_eff_dtime", $"sd_process_list_eff_dtime".cast("timestamp"))
                                                .withColumn("sd_process_inst_list_eff_dtime", $"sd_process_inst_list_eff_dtime".cast("timestamp"))

    if (!DF5_LoadReadySet_PITSrvcDlvry.rdd.isEmpty()) {

      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table s_srvc_dlvry_pit")

      /* val sql_Delete_PIT_SrvcDlvry = "delete from mi_dwb.s_srvc_dlvry_pit where srvc_dlvry_hk=?"

       DbHelper.deleteDbRecsForInput( inputDF = DF5_LoadReadySet_PITSrvcDlvry.select("srvc_dlvry_hk").distinct()
                                    , dbConfig = dbConfig
                                    , deleteSql = sql_Delete_PIT_SrvcDlvry)
      */

      log.info("[SrvcDlvryBaseVaultJob] Calling DbProc=batch_delete_srvcdlvry_pit")

      DbHelper.callDbProc( dbConfig = dbConfig ,
                           procName = "batch_delete_srvcdlvry_pit")


      log.info("[SrvcDlvryBaseVaultJob] Writing records to DB table mi_dwb.s_srvc_dlvry_pit")

      DbHelper.writeDF( dbConfig = dbConfig
        , tabName  = "mi_dwb.s_srvc_dlvry_pit"
        , df       = DF5_LoadReadySet_PITSrvcDlvry)


    } else {
      log.info("[SrvcDlvryBaseVaultJob] RDD Empty - ZERO new records written to DB table s_srvc_dlvry_pit")
    }

  }
}
