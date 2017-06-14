package uk.gov.ipt.mi.dw.identity

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import uk.gov.ipt.mi.dw.{DbConfig, DbHelper, MIIdentitySatVaultConfig}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.expressions.Window
import uk.gov.ipt.mi.model._


class IdentitySatVaultJob(batchId: String, timestampStr: String, spark: SparkSession) {

	val log = LoggerFactory.getLogger(IdentitySatVaultJob.this.getClass)

	def start(dbConfig: DbConfig, identitySatVaultConfig: MIIdentitySatVaultConfig): Unit = {

		val sparkSession = SparkSession.builder.getOrCreate()
		import sparkSession.implicits._

		val sat_id_info_cols = "sat_id_info_cols"
		val tmp_identity_bsn_keys_tabName = "mi_dwb.tmp_identity_bsn_keys"
		val dw_sat_chg_log_tabName = "mi_dwb.s_identity_change_log"
		val dw_sat_vis_tabName = "mi_dwb.s_identity_visibility"
		val dw_sat_ext_handle_tabName = "mi_dwb.s_identity_external_handle"
		val dw_sat_biom_info_tabName = "mi_dwb.s_identity_biometric_info"
		val dw_sat_ref_tabName = "mi_dwb.s_identity_reference"
		val dw_sat_cond_tabName = "mi_dwb.s_identity_condition"
		val dw_sat_biog_set_mem_tabName = " mi_dwb.s_identity_biog_set_member"
		val dw_sat_desc_set_mem_tabName = "mi_dwb.s_identity_descr_set_member"
		val dw_sat_med_set_member_tabName = "mi_dwb.s_identity_media_set_member"
		val dw_sat_pit_tabName = "mi_dwb.s_identity_pit"
		val dw_lnk_id_person_tabName = "mi_dwb.lnk_identity_person"
		val dw_id_person_lnk_pit_tabName = "mi_dwb.s_identity_person_lnk_pit"
		val dw_coll_cntr_tabName = "mi_dwb.dv_hash_key_collision_counter"
		val dw_int_s_identity_pit_tabName = "mi_dwb.int_s_identity_pit"

		// ------------------------------------- Common Code for Satellites---------------------------------------------

		var idLnkPersonDF = spark.emptyDataset[LinkIdentityPerson].toDF
		var idSatInfoDF = spark.emptyDataset[SIdentityInfo].toDF
		var idSatBiometricInfoDF = spark.emptyDataset[SIdentityBiometricInfo].toDF
		var idSatReferenceDF = spark.emptyDataset[SIdentityReference].toDF
		var idSatConditionDF = spark.emptyDataset[SIdentityCondition].toDF
		var idSatBiographicsDF = spark.emptyDataset[SIdentityBiographics].toDF
		var idSatDescrSetDF = spark.emptyDataset[SIdentityDescriptors].toDF
		var idSatMediaSetDF = spark.emptyDataset[SIdentityMedia].toDF

		try {
			idLnkPersonDF = spark.read.json(identitySatVaultConfig.lnkIdentityPerInputPath)

		} catch {
			case ex: AnalysisException => {
				idLnkPersonDF = spark.emptyDataset[LinkIdentityPerson].toDF
			}
		}


		try {
			idSatInfoDF = spark.read.json(identitySatVaultConfig.satIdentityInfoInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatInfoDF = spark.emptyDataset[SIdentityInfo].toDF
			}
		}

		try {
			idSatBiometricInfoDF = spark.read.json(identitySatVaultConfig.satIdentityBiomInfoInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatBiometricInfoDF = spark.emptyDataset[SIdentityBiometricInfo].toDF
			}
		}

		try {
			idSatReferenceDF = spark.read.json(identitySatVaultConfig.satIdentityRefInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatReferenceDF = spark.emptyDataset[SIdentityReference].toDF
			}
		}

		try {
			idSatConditionDF = spark.read.json(identitySatVaultConfig.satIdentityCondInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatConditionDF = spark.emptyDataset[SIdentityCondition].toDF
			}
		}

		try {
			idSatBiographicsDF = spark.read.json(identitySatVaultConfig.satIdentityBiogInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatBiographicsDF = spark.emptyDataset[SIdentityBiographics].toDF
			}
		}

		try {
			idSatDescrSetDF = spark.read.json(identitySatVaultConfig.satIdentityDescrInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatDescrSetDF = spark.emptyDataset[SIdentityDescriptors].toDF
			}
		}

		try {
			idSatMediaSetDF = spark.read.json(identitySatVaultConfig.satIdentityMedInputPath)

		} catch {
			case ex: AnalysisException => {
				idSatMediaSetDF = spark.emptyDataset[SIdentityMedia].toDF
			}
		}

		val getIdentityBsnKeyDF = idSatInfoDF.select("identity_handle_id", "record_source")
		  .union(idSatBiometricInfoDF.select("identity_handle_id", "record_source")
			.union(idSatReferenceDF.select("identity_handle_id", "record_source")
			  .union(idSatConditionDF.select("identity_handle_id", "record_source")
				.union(idSatBiographicsDF.select("identity_handle_id", "record_source")
				  .union(idSatMediaSetDF.select("identity_handle_id", "record_source")
					.union(idSatDescrSetDF.select("identity_handle_id", "record_source"))))))).distinct

		DbHelper.writeDF(dbConfig, tmp_identity_bsn_keys_tabName, getIdentityBsnKeyDF)

		val sqlGetDistinctIdBsnKey = "(select identity_hk, effective_datetime, record_source, identity_handle_id, id_vsblty_rec_hash_value, id_vsblty_eff_dtime, id_ext_hndl_rec_hash_value, id_ext_hndl_eff_dtime, id_biom_list_agg_hash, id_biom_list_eff_dtime, id_ref_list_agg_hash, id_ref_list_eff_dtime, id_cond_list_agg_hash, id_cond_list_eff_dtime, id_biog_superset_agg_hash, id_descr_superset_agg_hash, id_media_superset_agg_hash, latest_rec_flag from mi_dwb.vw_get_identity_pit) as subset"

		val allIdBsnKeyDF = DbHelper.databaseDF(dbConfig, sqlGetDistinctIdBsnKey)(spark)

		val distinctIdBsnKeyDF = allIdBsnKeyDF.select("identity_hk", "record_source", "identity_handle_id").distinct

		// ------------------------------------- identity info Data Set Start ----------------------------------

		val idInfoDataSetMidDF = idSatInfoDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
		  .select("b.identity_hk", "a.*")

		val idInfoDataSetDF = DbHelper.addMissingFields(idInfoDataSetMidDF, sat_id_info_cols)
		  .withColumn("data_status", lit("N"))

		// ------------------------------------- identity info Data Set End ----------------------------------

		// ------------------------------------- s_identity_change_log Start ----------------------------------

		val insertIdSatChgLogDF = idInfoDataSetDF.withColumnRenamed("bsn_event_datetime", "effective_datetime")
		  .withColumnRenamed("message_id", "src_message_id")
		  .withColumnRenamed("chlg_rec_hash_value", "rec_hash_value")
		  .withColumn("dv_created_by_batch_id", lit(batchId))
		  .withColumn("dv_created_datetime", lit(timestampStr))
		  .select("identity_hk", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "src_message_id", "src_cd", "src_created_by", "src_created_datetime", "aud_created_by", "aud_created_datetime", "aud_last_updated_by", "aud_last_updated_datetime")

		DbHelper.writeDF(dbConfig, dw_sat_chg_log_tabName, insertIdSatChgLogDF)

		// ------------------------------------- s_identity_change_log End -----------------------------------

		//--------------------------------------- s_identity_visibility Start --------------------------------

		val incomingIdVisColDF = idInfoDataSetDF.select("identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "vis_rec_hash_value", "identity_handle_visibility", "data_status")

		val currentIdVisColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
		  .select("identity_hk", "record_source", "identity_handle_id", "id_vsblty_rec_hash_value", "effective_datetime")
		  .withColumnRenamed("id_vsblty_rec_hash_value", "vis_rec_hash_value")
		  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
		  .withColumn("identity_handle_visibility", lit(null).cast(StringType))
		  .withColumn("data_status", lit("C"))
		  .select("identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "vis_rec_hash_value", "identity_handle_visibility", "data_status")

		val tmpPersonVisHksSatDF = incomingIdVisColDF.union(currentIdVisColDF)

		tmpPersonVisHksSatDF.createOrReplaceTempView("vw_tmpIdentityVisHksSatDF")

		val sqlCompressVisHk = "select iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.vis_rec_hash_value as vis_rec_hash_value, iset.identity_handle_visibility as identity_handle_visibility from(select identity_hk, record_source, identity_handle_id, bsn_event_datetime, vis_rec_hash_value, identity_handle_visibility, data_status ,lag(vis_rec_hash_value,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_vis_rec_hash_value from vw_tmpIdentityVisHksSatDF) iset where iset.data_status = 'N' and (iset.prev_vis_rec_hash_value is null or iset.vis_rec_hash_value != iset.prev_vis_rec_hash_value)"

		val insertIdentitySatVisDF = spark.sql(sqlCompressVisHk)
		  .withColumn("dv_created_by_batch_id", lit(batchId))
		  .withColumn("dv_created_datetime", lit(timestampStr))
		  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
		  .withColumnRenamed("vis_rec_hash_value", "rec_hash_value")
		  .select("identity_hk", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "identity_handle_visibility")

		DbHelper.writeDF(dbConfig, dw_sat_vis_tabName, insertIdentitySatVisDF)

		//--------------------------------------- s_identity_visibility End -----------------------------------

		//------------------------------------- s_identity_external_handle Start ---------------------------------

			val incomingIdExtHandleColDF = idInfoDataSetDF.select("identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "eh_rec_hash_value", "eh_external_handle_value", "eh_external_handle_space", "data_status")

			val currentIdExtHandleColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
			  .select("identity_hk", "record_source", "identity_handle_id", "id_ext_hndl_rec_hash_value", "effective_datetime")
			  .withColumnRenamed("id_ext_hndl_rec_hash_value", "eh_rec_hash_value")
			  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
			  .withColumn("eh_external_handle_value", lit(null).cast(StringType))
			  .withColumn("eh_external_handle_space", lit(null).cast(StringType))
			  .withColumn("data_status", lit("C"))
			  .select("identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "eh_rec_hash_value", "eh_external_handle_value", "eh_external_handle_space", "data_status")

			val tmpPersonExtHandleHksSatDF = incomingIdExtHandleColDF.union(currentIdExtHandleColDF)

			tmpPersonExtHandleHksSatDF.createOrReplaceTempView("vw_tmpIdentityExtHandleHksSatDF")

			val sqlCompressExtHandleHk = "select iset.identity_hk as identity_hk, iset.record_source as record_source, iset.bsn_event_datetime as bsn_event_datetime, iset.eh_rec_hash_value as eh_rec_hash_value, iset.eh_external_handle_value as eh_external_handle_value, iset.eh_external_handle_space as eh_external_handle_space from(select identity_hk, record_source, identity_handle_id, bsn_event_datetime, eh_rec_hash_value, eh_external_handle_value, eh_external_handle_space, data_status ,lag(eh_rec_hash_value,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_eh_rec_hash_value from vw_tmpIdentityExtHandleHksSatDF) iset where iset.data_status = 'N' and (iset.prev_eh_rec_hash_value is null or iset.eh_rec_hash_value != iset.prev_eh_rec_hash_value)"

			val insertIdSatExtHandleDF = spark.sql(sqlCompressExtHandleHk)
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .withColumnRenamed("eh_rec_hash_value", "rec_hash_value")
			  .withColumnRenamed("eh_external_handle_value", "external_handle_value")
			  .withColumnRenamed("eh_external_handle_space", "external_handle_space")
			  .select("identity_hk", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "external_handle_value", "external_handle_space")

			DbHelper.writeDF(dbConfig, dw_sat_ext_handle_tabName, insertIdSatExtHandleDF)

		//------------------------------------- s_identity_external_handle End ---------------------------------

		//-------------------------------------- s_identity_biometric_info Start ------------------------------

			val idBiomInfoDataSetMidDF = idSatBiometricInfoDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
			  .select("b.identity_hk", "a.*")
			  .withColumn("data_status", lit("N"))

			val idBiomInfoDataSetDF = idBiomInfoDataSetMidDF.filter($"rec_seqno" === "0")
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "biometric_list_agg_hash", "data_status")

			val currentIdBiomInfoColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
			  .withColumn("message_id", lit(null).cast(StringType))
			  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
			  .withColumnRenamed("id_biom_list_agg_hash", "biometric_list_agg_hash")
			  .withColumn("data_status", lit("C"))
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "biometric_list_agg_hash", "data_status")

			val tmpIdBiomInfoHksSatDF = idBiomInfoDataSetDF.union(currentIdBiomInfoColDF)

			tmpIdBiomInfoHksSatDF.createOrReplaceTempView("vw_tmpIdentityBiomInfoHksSatDF")

			val sqlCompressBiomInfoHk = "select iset.message_id as message_id, iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.biometric_list_agg_hash as biometric_list_agg_hash from(select message_id, identity_hk, record_source, identity_handle_id, bsn_event_datetime, biometric_list_agg_hash, data_status ,lag(biometric_list_agg_hash,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_biometric_list_agg_hash from vw_tmpIdentityBiomInfoHksSatDF)iset where iset.data_status = 'N' and (iset.prev_biometric_list_agg_hash is null or iset.biometric_list_agg_hash != iset.prev_biometric_list_agg_hash)"

			val idIdBiomInfoColDF = spark.sql(sqlCompressBiomInfoHk)

			val insertIdBiomInfoMidDF = idIdBiomInfoColDF.as('a).join(idBiomInfoDataSetMidDF.as('b), $"a.identity_hk" === $"b.identity_hk" && $"a.message_id" === $"b.message_id" && $"a.bsn_event_datetime" === $"b.bsn_event_datetime")
			  .select("b.*").distinct

			val insertIdBiomInfoDF = DbHelper.addMissingFields(insertIdBiomInfoMidDF, dw_sat_biom_info_tabName)
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .select("identity_hk", "biometric_handle_id", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "biometric_handle_visibility", "biometric_ext_handle_value", "biometric_ext_handle_space", "biometric_nature", "biometric_type_cd", "biometric_value", "biometric_created_by", "biometric_created_datetime")

			DbHelper.writeDF(dbConfig, dw_sat_biom_info_tabName, insertIdBiomInfoDF)

		//-------------------------------------- s_identity_biometric_info End --------------------------------

		//-------------------------------------- s_identity_reference Start -----------------------------------

			val idRefDataSetMidDF = idSatReferenceDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
			  .select("b.identity_hk", "a.*")
			  .withColumn("data_status", lit("N"))

			val idRefDataSetDF = idRefDataSetMidDF.filter($"rec_seqno" === "0")
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "reference_list_agg_hash", "data_status")

			val currentIdRefColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
			  .withColumn("message_id", lit(null).cast(StringType))
			  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
			  .withColumnRenamed("id_ref_list_agg_hash", "reference_list_agg_hash")
			  .withColumn("data_status", lit("C"))
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "reference_list_agg_hash", "data_status")

			val tmpIdRefHksSatDF = idRefDataSetDF.union(currentIdRefColDF)

			tmpIdRefHksSatDF.createOrReplaceTempView("vw_tmpIdentityRefHksSatDF")

			val sqlCompressRefHk = "select iset.message_id as message_id, iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.reference_list_agg_hash as reference_list_agg_hash from(select message_id, identity_hk, record_source, identity_handle_id, bsn_event_datetime, reference_list_agg_hash, data_status ,lag(reference_list_agg_hash,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_reference_list_agg_hash from vw_tmpIdentityRefHksSatDF)iset where iset.data_status = 'N' and (iset.prev_reference_list_agg_hash is null or iset.reference_list_agg_hash != iset.prev_reference_list_agg_hash)"

			val idIdRefColDF = spark.sql(sqlCompressRefHk)

			val insertIdRefMidDF = idIdRefColDF.as('a).join(idRefDataSetMidDF.as('b), $"a.identity_hk" === $"b.identity_hk" && $"a.message_id" === $"b.message_id" && $"a.bsn_event_datetime" === $"b.bsn_event_datetime")
			  .select("b.*").distinct

			val insertIdRefDF = DbHelper.addMissingFields(insertIdRefMidDF, dw_sat_ref_tabName)
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .select("identity_hk", "reference_handle_id", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "reference_visibility", "reference_ext_handle_value", "reference_ext_handle_space", "reference_type_cd", "reference_value", "reference_created_by", "reference_created_datetime")

			DbHelper.writeDF(dbConfig, dw_sat_ref_tabName, insertIdRefDF)

		//-------------------------------------- s_identity_reference End -------------------------------------

		//-------------------------------------- s_identity_condition Start -----------------------------------

			val idCondDataSetMidDF = idSatConditionDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
			  .select("b.identity_hk", "a.*")
			  .withColumn("data_status", lit("N"))

			val idCondDataSetDF = idCondDataSetMidDF.filter($"rec_seqno" === "0")
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "condition_list_agg_hash", "data_status")

			val currentIdCondColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
			  .withColumn("message_id", lit(null).cast(StringType))
			  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
			  .withColumnRenamed("id_cond_list_agg_hash", "condition_list_agg_hash")
			  .withColumn("data_status", lit("C"))
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "condition_list_agg_hash", "data_status")

			val tmpIdCondHksSatDF = idCondDataSetDF.union(currentIdCondColDF)

			tmpIdCondHksSatDF.createOrReplaceTempView("vw_tmpIdentityCondHksSatDF")

			val sqlCompressCondHk = "select iset.message_id as message_id, iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.condition_list_agg_hash as condition_list_agg_hash from(select message_id, identity_hk, record_source, identity_handle_id, bsn_event_datetime, condition_list_agg_hash, data_status ,lag(condition_list_agg_hash,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_condition_list_agg_hash from vw_tmpIdentityCondHksSatDF)iset where iset.data_status = 'N' and (iset.prev_condition_list_agg_hash is null or iset.condition_list_agg_hash != iset.prev_condition_list_agg_hash)"

			val idIdCondColDF = spark.sql(sqlCompressCondHk)

			val insertIdCondMidDF = idIdCondColDF.as('a).join(idCondDataSetMidDF.as('b), $"a.identity_hk" === $"b.identity_hk" && $"a.message_id" === $"b.message_id" && $"a.bsn_event_datetime" === $"b.bsn_event_datetime")
			  .select("b.*").distinct

			val insertIdCondDF = DbHelper.addMissingFields(insertIdCondMidDF, dw_sat_cond_tabName)
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .select("identity_hk", "condition_handle_id", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "condition_handle_visibility", "condition_ext_handle_value", "condition_ext_handle_space", "condition_start_datetime", "condition_end_datetime", "condition_type_cd", "condition_update_datetime", "condition_note", "condition_created_by", "condition_created_datetime", "condition_originated_by")

			DbHelper.writeDF(dbConfig, dw_sat_cond_tabName, insertIdCondDF)

		//--------------------------------------- s_identity_condition End -----------------------------------------

		//--------------------------------------- s_identity_biog_set_member Start ---------------------------------

		//use effective_datetime instead of individual dates in PIT
		// include message_id

			val incomingBiogDF = idSatBiographicsDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
			  .select("b.identity_hk", "a.*")
			  .withColumn("data_status", lit("N"))

			val idBiogDataSetDF = incomingBiogDF.filter($"biographic_rec_seqno" === "0").distinct
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "biog_superset_agg_hash", "data_status")

			// include message _id for list tables - condition, ref, biometric etc where joining back
			val currentIdBiogColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
			  .withColumn("message_id", lit(null).cast(StringType))
			  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
			  .withColumnRenamed("id_biog_superset_agg_hash", "biog_superset_agg_hash")
			  .withColumn("data_status", lit("C"))
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "biog_superset_agg_hash", "data_status")

			val tmpIdBiogHksSatDF = idBiogDataSetDF.union(currentIdBiogColDF)

			tmpIdBiogHksSatDF.createOrReplaceTempView("vw_tmpIdentityBiogHksSatDF")

			val sqlCompressBiogHk = "select iset.message_id as message_id, iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.biog_superset_agg_hash as biog_superset_agg_hash from(select message_id, identity_hk, record_source, identity_handle_id, bsn_event_datetime, biog_superset_agg_hash, data_status ,lag(biog_superset_agg_hash,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_biog_superset_agg_hash from vw_tmpIdentityBiogHksSatDF) iset where iset.data_status = 'N' and (iset.prev_biog_superset_agg_hash is null or iset.biog_superset_agg_hash != iset.prev_biog_superset_agg_hash)"

			val idIdBiogColDF = spark.sql(sqlCompressBiogHk)

			val insertIdBiogMidDF = idIdBiogColDF.as('a).join(incomingBiogDF.as('b), $"a.identity_hk" === $"b.identity_hk" && $"a.message_id" === $"b.message_id" && $"a.bsn_event_datetime" === $"b.bsn_event_datetime")
			  .select("b.*").distinct

			val insertIdBiogDF = DbHelper.addMissingFields(insertIdBiogMidDF, dw_sat_biog_set_mem_tabName)
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .withColumnRenamed("biographic_rec_hash_value", "rec_hash_value")
			  .select("identity_hk", "biog_set_handle_id", "biographic_handle_id", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "biographic_handle_visibility", "biographic_type_cd", "biographic_value", "biographic_value_type_cd", "biographic_created_by", "biographic_created_datetime", "biographic_reference_data_set", "biog_set_rec_hash_value", "biog_set_handle_visibility", "biog_set_ext_handle_value", "biog_set_ext_handle_space", "biog_set_purpose_cd", "biog_set_created_by", "biog_set_created_datetime", "biog_set_agg_hash", "biog_superset_agg_hash")

			DbHelper.writeDF(dbConfig, dw_sat_biog_set_mem_tabName, insertIdBiogDF)

		//--------------------------------------- s_identity_biog_set_member End  ---------------------------------

		//--------------------------------------- s_identity_descr_set_member Start ---------------------------------

			val incomingDescrDF = idSatDescrSetDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
			  .select("b.identity_hk", "a.*")
			  .withColumn("data_status", lit("N"))

			val idDescrDataSetDF = incomingDescrDF.filter($"descr_rec_seqno" === "0").distinct
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "descr_superset_agg_hash", "data_status")

			val currentIdDescrColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
			  .withColumn("message_id", lit(null).cast(StringType))
			  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
			  .withColumnRenamed("id_descr_superset_agg_hash", "descr_superset_agg_hash")
			  .withColumn("data_status", lit("C"))
			  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "descr_superset_agg_hash", "data_status")

			val tmpIdDescrHksSatDF = idDescrDataSetDF.union(currentIdDescrColDF)

			tmpIdDescrHksSatDF.createOrReplaceTempView("vw_tmpIdentityDescrHksSatDF")

			val sqlCompressDescrHk = "select iset.message_id as message_id, iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.descr_superset_agg_hash as descr_superset_agg_hash from(select message_id, identity_hk, record_source, identity_handle_id, bsn_event_datetime, descr_superset_agg_hash, data_status ,lag(descr_superset_agg_hash,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_descr_superset_agg_hash from vw_tmpIdentityDescrHksSatDF)iset where iset.data_status = 'N' and (iset.prev_descr_superset_agg_hash is null or iset.descr_superset_agg_hash != iset.prev_descr_superset_agg_hash)"

			val idIdDescrColDF = spark.sql(sqlCompressDescrHk)

			val insertIdDescrMidDF = idIdDescrColDF.as('a).join(incomingDescrDF.as('b), $"a.identity_hk" === $"b.identity_hk" && $"a.message_id" === $"b.message_id" && $"a.bsn_event_datetime" === $"b.bsn_event_datetime")
			  .select("b.*").distinct

			val insertIdDescrDF = DbHelper.addMissingFields(insertIdDescrMidDF, dw_sat_desc_set_mem_tabName)
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .withColumnRenamed("descr_rec_hash_value", "rec_hash_value")
			  .select("identity_hk", "descr_set_handle_id", "descr_handle_id", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "descr_handle_visibility", "descr_type_cd", "descr_value", "descr_created_by", "descr_created_datetime", "descr_set_rec_hash_value", "descr_set_handle_visibility", "descr_set_ext_handle_value", "descr_set_ext_handle_space", "descr_set_created_by", "descr_set_created_datetime", "descr_set_agg_hash", "descr_superset_agg_hash")

			DbHelper.writeDF(dbConfig, dw_sat_desc_set_mem_tabName, insertIdDescrDF)

		//--------------------------------------- s_identity_descr_set_member End ---------------------------------

		//--------------------------------------- s_identity_media_set_member Start ---------------------------------

		val incomingMediaDF = idSatMediaSetDF.as('a).join(distinctIdBsnKeyDF.as('b), $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source")
		  .select("b.identity_hk", "a.*")
		  .withColumn("data_status", lit("N"))

		val idMediaDataSetDF = incomingMediaDF.filter($"media_rec_seqno" === "0").distinct
		  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "media_superset_agg_hash", "data_status")

		val currentIdMediaColDF = allIdBsnKeyDF.filter($"latest_rec_flag" === "Y")
		  .withColumn("message_id", lit(null).cast(StringType))
		  .withColumnRenamed("effective_datetime", "bsn_event_datetime")
		  .withColumnRenamed("id_media_superset_agg_hash", "media_superset_agg_hash")
		  .withColumn("data_status", lit("C"))
		  .select("message_id", "identity_hk", "record_source", "identity_handle_id", "bsn_event_datetime", "media_superset_agg_hash", "data_status")

		val tmpIdMediaHksSatDF = idMediaDataSetDF.union(currentIdMediaColDF)

		tmpIdMediaHksSatDF.createOrReplaceTempView("vw_tmpIdentityMediaHksSatDF")

		val sqlCompressMediaHk = "select iset.message_id as message_id, iset.identity_hk as identity_hk, iset.record_source as record_source, iset.identity_handle_id as identity_handle_id, iset.bsn_event_datetime as bsn_event_datetime, iset.media_superset_agg_hash as media_superset_agg_hash from(select message_id, identity_hk, record_source, identity_handle_id, bsn_event_datetime, media_superset_agg_hash, data_status ,lag(media_superset_agg_hash,1) over(partition by identity_hk order by bsn_event_datetime, data_status) prev_media_superset_agg_hash from vw_tmpIdentityMediaHksSatDF)iset where iset.data_status = 'N' and (iset.prev_media_superset_agg_hash is null or iset.media_superset_agg_hash != iset.prev_media_superset_agg_hash)"

		val idIdMediaColDF = spark.sql(sqlCompressMediaHk)

		val insertIdMediaMidDF = idIdMediaColDF.as('a).join(incomingMediaDF.as('b), $"a.identity_hk" === $"b.identity_hk" && $"a.message_id" === $"b.message_id" && $"a.bsn_event_datetime" === $"b.bsn_event_datetime")
		  .select("b.*").distinct

		val insertIdMediaDF = DbHelper.addMissingFields(insertIdMediaMidDF, dw_sat_med_set_member_tabName)
		  .withColumn("dv_created_by_batch_id", lit(batchId))
		  .withColumn("dv_created_datetime", lit(timestampStr))
		  .withColumnRenamed("bsn_event_datetime", "effective_datetime")
		  .withColumnRenamed("media_rec_hash_value", "rec_hash_value")
		  .select("identity_hk", "media_set_handle_id", "media_handle_id", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "media_handle_visibility", "media_type_cd", "media_file_handle_id", "media_created_by", "media_created_datetime", "media_set_rec_hash_value", "media_set_handle_visibility", "media_set_ext_handle_value", "media_set_ext_handle_space", "media_set_created_by", "media_set_created_datetime", "media_set_agg_hash", "media_superset_agg_hash")

		DbHelper.writeDF(dbConfig, dw_sat_med_set_member_tabName, insertIdMediaDF)

			//--------------------------------------- s_identity_media_set_member End ---------------------------------


			//-------------------------------------- s_identity_pit Start -----------------------------------------

			val idPit1Df = idSatInfoDF.as('a).join(idSatBiometricInfoDF.select("message_id","identity_handle_id","record_source","biometric_list_agg_hash").filter($"rec_seqno"==="0").distinct.as('b), $"a.message_id" === $"b.message_id" && $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source","leftouter")
			  .select("a.message_id","a.identity_handle_id","a.record_source","a.bsn_event_datetime","a.vis_rec_hash_value","a.eh_rec_hash_value","b.biometric_list_agg_hash")
			val idPit2Df = idPit1Df.as('a).join(idSatReferenceDF.select("message_id","identity_handle_id","record_source","reference_list_agg_hash").filter($"rec_seqno"==="0").distinct.as('b), $"a.message_id" === $"b.message_id" && $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source","leftouter")
			  .select("a.*","b.reference_list_agg_hash")
			val idPit3Df = idPit2Df.as('a).join(idSatConditionDF.select("message_id","identity_handle_id","record_source","condition_list_agg_hash").filter($"rec_seqno"==="0").distinct.as('b), $"a.message_id" === $"b.message_id" && $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source","leftouter")
			  .select("a.*","b.condition_list_agg_hash")
			val idPit4Df = idPit3Df.as('a).join(idSatBiographicsDF.select("message_id","identity_handle_id","record_source","biog_superset_agg_hash").filter($"biographic_rec_seqno"==="0").distinct.as('b), $"a.message_id" === $"b.message_id" && $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source","leftouter")
			  .select("a.*","b.biog_superset_agg_hash")
			val idPit5Df = idPit4Df.as('a).join(idSatDescrSetDF.select("message_id","identity_handle_id","record_source","descr_superset_agg_hash").filter($"descr_rec_seqno"==="0").distinct.as('b), $"a.message_id" === $"b.message_id" && $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source","leftouter")
			  .select("a.*","b.descr_superset_agg_hash")
			val idPit6Df = idPit5Df.as('a).join(idSatMediaSetDF.select("message_id","identity_handle_id","record_source","media_superset_agg_hash").filter($"media_rec_seqno"==="0").distinct.as('b), $"a.message_id" === $"b.message_id" && $"a.identity_handle_id" === $"b.identity_handle_id" && $"a.record_source" === $"b.record_source","leftouter")
			  .select("a.*","b.media_superset_agg_hash")
			  .withColumnRenamed("message_id","src_message_id")
			  .withColumnRenamed("vis_rec_hash_value","id_vsblty_rec_hash_value")
			  .withColumnRenamed("eh_rec_hash_value","id_ext_hndl_rec_hash_value")
			  .withColumnRenamed("biometric_list_agg_hash","id_biom_list_agg_hash")
			  .withColumnRenamed("reference_list_agg_hash","id_ref_list_agg_hash")
			  .withColumnRenamed("condition_list_agg_hash","id_cond_list_agg_hash")
			  .withColumnRenamed("biog_superset_agg_hash","id_biog_superset_agg_hash")
			  .withColumnRenamed("descr_superset_agg_hash","id_descr_superset_agg_hash")
			  .withColumnRenamed("media_superset_agg_hash","id_media_superset_agg_hash")
			  .select("src_message_id","record_source","identity_handle_id","bsn_event_datetime","id_vsblty_rec_hash_value","id_ext_hndl_rec_hash_value","id_biom_list_agg_hash","id_ref_list_agg_hash","id_cond_list_agg_hash","id_biog_superset_agg_hash","id_descr_superset_agg_hash","id_media_superset_agg_hash")

			DbHelper.writeDF(dbConfig, dw_int_s_identity_pit_tabName, idPit6Df)

			val sqlGetIdPitHashKey = "(select identity_hk,effective_datetime,expiry_datetime,id_vsblty_eff_dtime,id_vsblty_rec_hash_value,id_ext_hndl_eff_dtime,id_ext_hndl_rec_hash_value,id_biom_list_eff_dtime,id_biom_list_agg_hash,id_ref_list_eff_dtime,id_ref_list_agg_hash,id_cond_list_eff_dtime,id_cond_list_agg_hash,id_biog_superset_eff_dtime,id_biog_superset_agg_hash,id_descr_superset_eff_dtime,id_descr_superset_agg_hash,id_media_superset_eff_dtime,id_media_superset_agg_hash from mi_dwb.vw_resolve_identity_pit) as subset"

			val insertIdPitMidDF = DbHelper.databaseDF(dbConfig, sqlGetIdPitHashKey)(spark)

			val insertIdPitDF = insertIdPitMidDF.withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumn("dv_last_updated_by_batch_id", lit(batchId))
			  .withColumn("dv_last_updated_datetime", lit(timestampStr))

			// select("identity_hk","effective_datetime","expiry_datetime","id_vsblty_eff_dtime","id_vsblty_rec_hash_value","id_ext_hndl_eff_dtime","id_ext_hndl_rec_hash_value","id_biom_list_eff_dtime","id_biom_list_agg_hash","id_ref_list_eff_dtime","id_ref_list_agg_hash","id_cond_list_eff_dtime","id_cond_list_agg_hash", "id_biog_superset_agg_hash", "id_descr_superset_agg_hash","id_media_superset_agg_hash","dv_created_by_batch_id","dv_created_datetime","dv_last_updated_by_batch_id","dv_last_updated_datetime")

			DbHelper.writeDF(dbConfig, dw_sat_pit_tabName, insertIdPitDF)

			//-------------------------------------- s_identity_pit End -------------------------------------------


			//-------------------------------------- lnk_identity_person Start ------------------------------------

			val insertIdLnkPersonHksDF = fnFilterIdLnkPerHash(idLnkPersonDF, dbConfig)

			insertIdLnkPersonHksDF.persist

			val insertIdLnkPersonDF = insertIdLnkPersonHksDF.filter($"insert_flag"==="Y")
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .withColumnRenamed("resolved_lnk_idntty_person_hk", "lnk_idntty_person_hk")
			  .select("lnk_idntty_person_hk", "effective_datetime", "record_source", "identity_hk", "person_hk", "dv_created_by_batch_id", "dv_created_datetime")

			DbHelper.writeDF(dbConfig, dw_lnk_id_person_tabName, insertIdLnkPersonDF)

			val insertCollisionCounter = insertIdLnkPersonHksDF.filter($"insert_flag"==="Y")
			  .filter($"counter_value">"1")
			  .withColumnRenamed("original_lnk_idntty_person_hk","hash_key")
			  .select("data_domain","hash_key","counter_value")

			DbHelper.writeDF(dbConfig, dw_coll_cntr_tabName, insertCollisionCounter)

			//-------------------------------------- lnk_identity_person End --------------------------------------


			//-------------------------------------- s_identity_person_lnk_pit Start ------------------------------

			//select("lnk_idntty_person_hk","effective_datetime","expiry_datetime","dv_created_by_batch_id","dv_created_datetime","dv_last_updated_by_batch_id","dv_last_updated_datetime")

			//DbHelper.writeDF(dbConfig, dw_id_person_lnk_pit_tabName, insertIdPersonLnkPitDF)

			//-------------------------------------- s_identity_person_lnk_pit End --------------------------------
	}

	def fnFilterIdLnkPerHash(inputDF: DataFrame, dbConfig: DbConfig): DataFrame = {

		inputDF.createOrReplaceTempView("vw_incomingIdLnkPerson")

		val sqlGetDistinctIdBsnKey = "select a.lnk_idntty_person_hk lnk_idntty_person_hk,a.record_source record_source,a.identity_handle_id identity_handle_id, a.person_handle_id person_handle_id, min(a.bsn_event_datetime) bsn_event_datetime from vw_incomingIdLnkPerson a group by  lnk_idntty_person_hk, record_source, identity_handle_id, person_handle_id"

		val distinctIdBsnKeyDF = spark.sql(sqlGetDistinctIdBsnKey)

		val int_identity_hub_tabName = "mi_dwb.int_cur_btch_id_lnk_per_bsn_key"

		DbHelper.writeDF(dbConfig, int_identity_hub_tabName, distinctIdBsnKeyDF)

		val sqlGetIdHashKey = "(select resolved_lnk_idntty_person_hk, original_lnk_idntty_person_hk, record_source, identity_handle_id, person_handle_id, identity_hk, person_hk, effective_datetime, insert_flag, data_domain, counter_value from mi_dwb.vw_cur_btch_id_lnk_per_hk) as subset"

		val currentIdentityHub = DbHelper.databaseDF(dbConfig, sqlGetIdHashKey)(spark)

		currentIdentityHub

	}

}


