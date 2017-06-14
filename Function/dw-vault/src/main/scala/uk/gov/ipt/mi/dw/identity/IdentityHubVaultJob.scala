package uk.gov..mi.dw.identity

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov..mi.dw.{DbConfig, DbHelper, MIIdentityHubVaultConfig}
import org.apache.spark.sql.AnalysisException

class IdentityHubVaultJob(batchId: String, timestampStr: String, spark: SparkSession) {

	def start(dbConfig: DbConfig, identityHubVaultConfig: MIIdentityHubVaultConfig): Unit = {

		val sparkSession = SparkSession.builder.getOrCreate()
		import sparkSession.implicits._
		val dw_hub_Id_tabName = "mi_dwb.hub_identity"
		val dw_coll_cntr_tabName = "mi_dwb.dv_hash_key_collision_counter"

		try {
				val incomingIdentityHubDF = spark.read.json(identityHubVaultConfig.hubIdentityInputPath)

				if (!incomingIdentityHubDF.rdd.isEmpty) {

					val insertIdentityHubHksDF = fnFilterHash(incomingIdentityHubDF, dbConfig)

					insertIdentityHubHksDF.persist

					val insertIdentityHubDF = insertIdentityHubHksDF.filter("insert_flag='Y'")
				  		.withColumn("dv_created_by_batch_id", lit(batchId))
				  		.withColumn("dv_created_datetime", lit(timestampStr))
				  		.select("identity_hk", "effective_datetime", "record_source", "identity_handle_id", "dv_created_by_batch_id", "dv_created_datetime")

					DbHelper.writeDF(dbConfig, dw_hub_Id_tabName, insertIdentityHubDF)

					insertIdentityHubHksDF.createOrReplaceTempView("vw_tmpinsertIdentityHubHksDF")

					val sqlGetCollisionCounter = "select data_domain, original_identity_hk, counter_value from vw_tmpinsertIdentityHubHksDF where insert_flag='Y' group by original_identity_hk having counter_value>1"

					val insertCollisionCounter = spark.sql(sqlGetCollisionCounter)
					  .withColumnRenamed("original_identity_hk","hash_key")
					  .select("data_domain","hash_key","counter_value")

					/*val insertCollisionCounter = insertIdentityHubHksDF.filter($"insert_flag='Y'").groupBy("original_identity_hk").filter("counter_value>1")
					  .withColumnRenamed("original_identity_hk","hash_key")
					  .select("data_domain","hash_key","counter_value")*/

					DbHelper.writeDF(dbConfig, dw_coll_cntr_tabName, insertCollisionCounter)

				}

		} catch {
			case ex: AnalysisException => {
				return
			}
		}

	}

	def fnFilterHash(inputDF: DataFrame, dbConfig: DbConfig): DataFrame = {

		inputDF.createOrReplaceTempView("vw_incomingIdentityHub")

		val sqlGetDistinctIdBsnKey = "select a.identity_hk identity_hk,a.record_source record_source,a.identity_handle_id identity_handle_id,min(a.bsn_event_datetime) bsn_event_datetime from vw_incomingIdentityHub a group by  identity_hk, record_source, identity_handle_id"

		val distinctIdBsnKeyDF = spark.sql(sqlGetDistinctIdBsnKey)

		val int_identity_hub_tabName = "mi_dwb.int_cur_btch_identity_bsn_key"

		DbHelper.writeDF(dbConfig, int_identity_hub_tabName, distinctIdBsnKeyDF)

		//val sqlGetIdHashKey = "(select identity_hk,effective_datetime,record_source,identity_handle_id,insert_flag from mi_dwb.vw_cur_btch_identity_hk) as subset"

		val sqlGetIdHashKey = "(select identity_hk, effective_datetime, record_source, identity_handle_id, counter_value, original_identity_hk, data_domain, insert_flag from mi_dwb.vw_cur_btch_identity_hk) as subset"

		val currentIdentityHub = DbHelper.databaseDF(dbConfig, sqlGetIdHashKey)(spark)

		currentIdentityHub

	}

}


