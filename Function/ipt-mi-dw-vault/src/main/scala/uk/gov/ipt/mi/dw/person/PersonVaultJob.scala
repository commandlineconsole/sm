package uk.gov.ipt.mi.dw.person

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ipt.mi.dw.{DbConfig, DbHelper, MIPersonVaultConfig, MIIdentityHubVaultConfig}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StringType
import uk.gov.ipt.mi.model._
class PersonVaultJob(batchId: String, timestampStr: String, spark: SparkSession) {

	def start(dbConfig: DbConfig, personVaultConfig: MIPersonVaultConfig): Unit = {

		val sparkSession = SparkSession.builder.getOrCreate()
		import sparkSession.implicits._
		val dw_hub_tabName = "mi_dwb.hub_person"
		val dw_sat_vis_tabName = "mi_dwb.s_person_visibility"
		val dw_sat_chg_log_tabName = "mi_dwb.s_person_change_log"
		val dw_hub_Id_tabName = "mi_dwb.hub_identity"
		val dw_lnk_person_id_tabName = "mi_dwb.lnk_person_identity"
		val dw_person_pit_tabName = "mi_dwb.s_person_pit"
		val expiry_datetime = "9999-12-31T24:00:00.000000"
		val per_vsblty_eff_dtime = lit(null).cast(StringType)
		val dv_last_updated_by_batch_id = lit(null).cast(StringType)
		val dv_last_updated_datetime = lit(null).cast(StringType)

		var incomingPersonHubDF = spark.emptyDataset[HubPerson].toDF
		var incomingPersonVisSatDF = spark.emptyDataset[PersonVisibility].toDF
		var incomingPersonChgLogSatDF = spark.emptyDataset[PersonChangeLog].toDF
		var incomingPersonLnkDF = spark.emptyDataset[LinkPerson].toDF

		try{
			incomingPersonHubDF = spark.read.json(personVaultConfig.hubPersonInputPath)

		} catch{
			case ex: AnalysisException => {
				incomingPersonHubDF = spark.emptyDataset[HubPerson].toDF
			}
		}

		try{
			incomingPersonVisSatDF = spark.read.json(personVaultConfig.satPersonVisInputPath)

		} catch{
			case ex: AnalysisException => {
				incomingPersonVisSatDF = spark.emptyDataset[PersonVisibility].toDF
			}
		}

		try{
			incomingPersonChgLogSatDF = spark.read.json(personVaultConfig.satPersonChgLogInputPath)

		} catch{
			case ex: AnalysisException => {
				incomingPersonChgLogSatDF = spark.emptyDataset[PersonChangeLog].toDF
			}
		}

		try{
			incomingPersonLnkDF = spark.read.json(personVaultConfig.lnkPersonIdInputPath)

		} catch{
			case ex: AnalysisException => {
				incomingPersonLnkDF = spark.emptyDataset[LinkPerson].toDF
			}
		}

			/* 	Comment-1 03-01-2017 Start
			 	Following part is moved to IdentityVaultJob 03-01-2017
			 	val incomingIdentityHubDF = spark.read.json(identityVaultConfig.hubIdentityInputPath)
				Comment-1 03-01-2017 End  */

			if (incomingPersonHubDF.rdd.isEmpty || incomingPersonVisSatDF.rdd.isEmpty || incomingPersonChgLogSatDF.rdd.isEmpty || incomingPersonLnkDF.rdd.isEmpty) {
				return
			}

			// ------------------------------------- hub_person Start ----------------------------------------------
			val insertPersonHubHksDF = fnFilterHash(incomingPersonHubDF, dbConfig)

			val insertPersonHubDF = insertPersonHubHksDF.filter("insert_flag='Y'")
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .select("person_hk", "effective_datetime", "record_source", "person_handle_id", "dv_created_by_batch_id", "dv_created_datetime")

			// ------------------------------------- hub_person End ------------------------------------------------

			// ------------------------------------- s_person_visibility Start -------------------------------------

			val incomingPersonVisHksSatDF = incomingPersonVisSatDF.as('a).join(insertPersonHubHksDF.as('b), $"a.record_source" === $"b.record_source" && $"a.person_handle_id" === $"b.person_handle_id")
			  .select("b.person_hk", "a.record_source", "a.person_handle_id", "a.rec_hash_value", "a.bsn_event_datetime", "a.person_handle_visibility")
			  .withColumn("data_status", lit("N"))

			/*	Comment-2 03-01-2017 Start
			 	The view definition for - mi_dwb.vw_pitPersonRecHashBsnKey, referred below, would be changed (db stuff only) as code for populating PIT would be placed now
				Comment-2 03-01-2017 End */

			val sqlGetPitRecHashBsnKey = "(select person_hk, record_source, person_handle_id, vis_rec_hash_value from mi_dwb.vw_pitPersonRecHashBsnKey) as subset"

			val curPitRecHashValsDF = DbHelper.databaseDF(dbConfig, sqlGetPitRecHashBsnKey)(spark)

			val curPitRecHashValsModDF = curPitRecHashValsDF.withColumn("bsn_event_datetime", lit("1000-01-01T00:00:00.000000"))
			  .withColumn("person_handle_visibility", lit(null))
			  .withColumn("data_status", lit("C"))

			val tmpPersonVisHksSatDF = incomingPersonVisHksSatDF.union(curPitRecHashValsModDF)

			tmpPersonVisHksSatDF.createOrReplaceTempView("vw_tmpPersonVisHksSatDF")

			val sqlCompressVisHks = "select iset.person_hk as person_hk, iset.bsn_event_datetime as effective_datetime, iset.record_source as record_source,iset.rec_hash_value as rec_hash_value,iset.person_handle_visibility as person_handle_visibility from(select person_hk,record_source,bsn_event_datetime,rec_hash_value, person_handle_visibility,data_status,lag(rec_hash_value,1) over(partition by person_hk order by bsn_event_datetime) prev_rec_hash_value from vw_tmpPersonVisHksSatDF) iset where iset.data_status = 'N' and (iset.prev_rec_hash_value is null or iset.rec_hash_value != iset.prev_rec_hash_value)"

			val getPersonSatVisDF = spark.sql(sqlCompressVisHks)

			val insertPersonSatVisDF = getPersonSatVisDF.withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .select("person_hk", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "person_handle_visibility")

			// ------------------------------------- s_person_visibility End ---------------------------------------

			// ------------------------------------- s_person_change_log Start -------------------------------------

			val insertPersonSatChgLogMidDF = incomingPersonChgLogSatDF.as('a).join(insertPersonHubHksDF.as('b), $"a.record_source" === $"b.record_source" && $"a.person_handle_id" === $"b.person_handle_id")
			  .select("b.person_hk", "a.*")
			  .withColumnRenamed("message_id", "src_message_id")
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))

			val insertPersonSatChgLogColsDF = DbHelper.addMissingFields(insertPersonSatChgLogMidDF, dw_sat_chg_log_tabName)

			val insertPersonSatChgLogDF = insertPersonSatChgLogColsDF.withColumnRenamed("bsn_event_datetime", "effective_datetime")
			  .select("person_hk", "effective_datetime", "record_source", "dv_created_by_batch_id", "dv_created_datetime", "rec_hash_value", "src_message_id", "src_person_space", "src_created_by", "src_created_datetime")

			// ------------------------------------- s_person_change_log End ---------------------------------------

			/* 	Comment-3 03-01-2017 Start
			   	commented as "Hub Identity" would be now populated from "mi.vault.lnk.person.identity.input"
				instead of mi.vault.identity.hub.input

			// ------------------------------------- hub_identity Start --------------------------------------------
			val insertIdentityHubHksDF = fnFilterIdHash(incomingIdentityHubDF, dbConfig)

			val insertIdentityHubMidDF = insertIdentityHubHksDF.filter("insert_flag='Y'")

			val insertIdentityHubDF = insertIdentityHubMidDF.withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .select("identity_hk", "effective_datetime", "record_source", "identity_handle_id", "dv_created_by_batch_id", "dv_created_datetime")

			// ------------------------------------- hub_identity End ----------------------------------------------
				Comment-3 03-01-2017 End  */

			/* 04-01-2017 following entire section commented - refer "comment-01" - Start


			//	Added-2 03-01-2017 Start
			// ------------------------------------- hub_identity Start --------------------------------------------
			//	Loading from DIR for lnk_person_identity
			val insertIdentityHubHksDF = fnFilterIdHash(incomingPersonLnkDF, dbConfig)

			val insertIdentityHubDF = insertIdentityHubHksDF.filter("insert_flag='Y'")
		      .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .select("identity_hk", "effective_datetime", "record_source", "identity_handle_id", "dv_created_by_batch_id", "dv_created_datetime")

			// ------------------------------------- hub_identity End ----------------------------------------------
			//	Added-2 03-01-2017 End

			// ------------------------------------- lnk_person_identity Start -------------------------------------

			incomingPersonLnkDF.createOrReplaceTempView("vw_incomingPersonLnkDF")

			val sqlGetDistinctPerLnkBsnKey = "select a.lnk_person_idntty_hk lnk_person_idntty_hk,a.record_source record_source,a.person_handle_id person_handle_id,a.identity_handle_id identity_handle_id, min(a.bsn_event_datetime) bsn_event_datetime from vw_incomingPersonLnkDF a group by  lnk_person_idntty_hk, record_source, person_handle_id, identity_handle_id"

			val distinctPerBsnKeyDF = spark.sql(sqlGetDistinctPerLnkBsnKey)

			val getPersonLnkBsnKeyDF = distinctPerBsnKeyDF.as('a).join(insertPersonHubHksDF.as('b), $"a.record_source" === $"b.record_source" && $"a.person_handle_id" === $"b.person_handle_id")
			  .select("a.lnk_person_idntty_hk", "a.bsn_event_datetime", "a.record_source", "a.identity_handle_id", "b.person_hk")

			val insertPersonLnkMidDF = getPersonLnkBsnKeyDF.as('a).join(insertIdentityHubHksDF.as('b), $"a.record_source" === $"b.record_source" && $"a.identity_handle_id" === $"b.identity_handle_id")
			  .withColumn("dv_created_by_batch_id", lit(batchId))
			  .withColumn("dv_created_datetime", lit(timestampStr))
			  .select("a.lnk_person_idntty_hk", "a.bsn_event_datetime", "a.record_source", "a.person_hk", "b.identity_hk","dv_created_by_batch_id","dv_created_datetime")

			val insertPersonLnkDF = insertPersonLnkMidDF.withColumnRenamed("bsn_event_datetime", "effective_datetime")

			// ------------------------------------- lnk_person_identity End ---------------------------------------

			04-01-2017 above entire section commented - refer "comment-01" - End */

			//	Added-3 03-01-2017 Start
			// ------------------------------------- s_person_pit Start --------------------------------------------

			// shall add column 'per_vsblty_rec_hash_value' to s_person_pit
			val insertPersonPitDF = insertPersonSatVisDF.withColumnRenamed("rec_hash_value","per_vsblty_rec_hash_value")
			  .withColumn("expiry_datetime",lit(expiry_datetime))
			  .withColumn("per_vsblty_eff_dtime",lit(per_vsblty_eff_dtime))
			  .withColumn("dv_last_updated_by_batch_id",lit(dv_last_updated_by_batch_id))
			  .withColumn("dv_last_updated_datetime",lit(dv_last_updated_datetime))
			  .select("person_hk","effective_datetime","expiry_datetime","per_vsblty_eff_dtime","per_vsblty_rec_hash_value","dv_created_by_batch_id","dv_created_datetime","dv_last_updated_by_batch_id","dv_last_updated_datetime")

			// ------------------------------------ s_person_pit End ------------------------------------------------
			//	Added-3 03-01-2017 End

			DbHelper.writeDF(dbConfig, dw_hub_tabName, insertPersonHubDF)

			DbHelper.writeDF(dbConfig, dw_sat_vis_tabName, insertPersonSatVisDF)

			DbHelper.writeDF(dbConfig, dw_sat_chg_log_tabName, insertPersonSatChgLogDF)

			// refer "comment-01"
			//DbHelper.writeDF(dbConfig, dw_hub_Id_tabName, insertIdentityHubDF)
			// refer "comment-01"
			//DbHelper.writeDF(dbConfig, dw_lnk_person_id_tabName, insertPersonLnkDF)

			DbHelper.writeDF(dbConfig, dw_person_pit_tabName, insertPersonPitDF)

	}

	def fnFilterHash(inputDF: DataFrame, dbConfig: DbConfig): DataFrame = {

		inputDF.createOrReplaceTempView("vw_incomingPersonHub")

		val sqlGetDistinctPerBsnKey = "select a.person_hk person_hk,a.record_source record_source,a.person_handle_id person_handle_id,min(a.bsn_event_datetime) bsn_event_datetime from vw_incomingPersonHub a group by  person_hk, record_source, person_handle_id"

		val distinctPerBsnKeyDF = spark.sql(sqlGetDistinctPerBsnKey)

		val int_hub_tabName = "mi_dwb.int_cur_btch_person_bsn_key"

		DbHelper.writeDF(dbConfig, int_hub_tabName, distinctPerBsnKeyDF)

		val sqlGetPerHashKey = "(select person_hk,effective_datetime,record_source,person_handle_id,insert_flag from mi_dwb.vw_cur_btch_person_hk) as subset"

		val currentPersonHub = DbHelper.databaseDF(dbConfig, sqlGetPerHashKey)(spark)

		currentPersonHub

	}

	def fnFilterIdHash(inputDF: DataFrame, dbConfig: DbConfig): DataFrame = {

		inputDF.createOrReplaceTempView("vw_incomingIdentityHub")

		// comment-01 - 04-jan-2017 : identity_hk & person_hk are not part of json in /apps/MI/stream/person/link/...
		val sqlGetDistinctIdBsnKey = "select a.identity_hk identity_hk,a.record_source record_source,a.identity_handle_id identity_handle_id,min(a.bsn_event_datetime) bsn_event_datetime from vw_incomingIdentityHub a group by  identity_hk, record_source, identity_handle_id"

		val distinctIdBsnKeyDF = spark.sql(sqlGetDistinctIdBsnKey)

		val int_identity_hub_tabName = "mi_dwb.int_cur_btch_identity_bsn_key"

		DbHelper.writeDF(dbConfig, int_identity_hub_tabName, distinctIdBsnKeyDF)

		val sqlGetIdHashKey = "(select identity_hk,effective_datetime,record_source,identity_handle_id,insert_flag from mi_dwb.vw_cur_btch_identity_hk) as subset"

		val currentIdentityHub = DbHelper.databaseDF(dbConfig, sqlGetIdHashKey)(spark)

		currentIdentityHub

	}

}


