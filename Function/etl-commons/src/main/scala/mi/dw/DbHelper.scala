package uk.gov..mi.dw

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object DbHelper {

  val log = LoggerFactory.getLogger(DbHelper.this.getClass)
  val placeNull = lit(null).cast(StringType)

  var personChangeLogMap = Map(("src_created_by",lit(placeNull)))
  var satIdInfoColsMap = Map(("src_created_by",lit(placeNull)),("src_person_space",lit(placeNull)),("aud_created_by",lit(placeNull)),("aud_created_datetime",lit(placeNull)),("aud_last_updated_by",lit(placeNull)),("aud_last_updated_datetime",lit(placeNull)))
  var biometricMap = Map(("biometric_created_by",lit(placeNull)))
  var referenceMap = Map(("reference_created_by",lit(placeNull)))
  var conditionMap = Map(("condition_type_cd",lit(placeNull)),("condition_originated_by",lit(placeNull)),("condition_created_by",lit(placeNull)),("condition_end_datetime",lit(placeNull)),("condition_update_datetime",lit(placeNull)))
  var mediaSetMemberMap = Map(("media_created_by",lit(placeNull)),("media_set_created_by",lit(placeNull)))
  var biogSetMemberMap = Map(("biographic_created_by",lit(placeNull)),("biographic_reference_data_set",lit(placeNull)),("biog_set_created_by",lit(placeNull)))
  var descrSetMemberMap = Map(("descr_created_by",lit(placeNull)),("descr_set_created_by",lit(placeNull)))

  val dw_per_sat_chg_log_tabName = "mi_dwb.s_person_change_log"
  val sat_id_info_cols = "sat_id_info_cols"
  val dw_id_biometric_tabName = "mi_dwb.s_identity_biometric_info"
  val dw_id_reference_tabName = "mi_dwb.s_identity_reference"
  val dw_id_condition_tabName = "mi_dwb.s_identity_condition"
  val dw_id_media_set_member_tabName = "mi_dwb.s_identity_media_set_member"
  val dw_sat_biog_set_mem_tabName =" mi_dwb.s_identity_biog_set_member"
  val dw_sat_desc_set_mem_tabName = "mi_dwb.s_identity_descr_set_member"

  def dbProps(dbConfig: DbConfig) : Properties = {
    val dbProperties = new Properties()
    dbProperties.put("user", dbConfig.user)
    dbProperties.put("password", dbConfig.passwd)
    dbProperties.put("driver","org.postgresql.Driver")
    dbProperties.put("ssl","false")
    dbProperties
  }

  def databaseDF(dbConfig: DbConfig, sqlQuery: String)(implicit spark: SparkSession) : DataFrame = {
    log.info(s"[DBHelper] reading database with query [$sqlQuery]")
    spark.read.jdbc(dbConfig.jdbcUrl, sqlQuery, dbProps(dbConfig))
  }
  
  def writeDF(dbConfig: DbConfig, tabName: String, df: DataFrame) = {
    log.info(s"[DBHelper] writing to database table[$tabName], with url [${dbConfig.jdbcUrl}]")
    df.write.mode("append").jdbc(dbConfig.jdbcUrl, tabName, dbProps(dbConfig))
  }

  def writeOverDF(dbConfig: DbConfig, tabName: String, df: DataFrame) = {
    log.info(s"[DBHelper] writing to database table[$tabName], with url [${dbConfig.jdbcUrl}]")
    df.write.mode("overwrite").jdbc(dbConfig.jdbcUrl, tabName, dbProps(dbConfig))
  }

  def addMissingFields(inputDF: DataFrame, tabName: String): DataFrame = {
    val colList = inputDF.columns
    var outputDF = inputDF

    if(tabName==dw_per_sat_chg_log_tabName) {
      personChangeLogMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==sat_id_info_cols) {
      satIdInfoColsMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==dw_id_biometric_tabName) {
      biometricMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==dw_id_reference_tabName) {
      referenceMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==dw_id_condition_tabName) {
      conditionMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==dw_id_media_set_member_tabName) {
      mediaSetMemberMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==dw_sat_biog_set_mem_tabName) {
      biogSetMemberMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
    }

    if(tabName==dw_sat_desc_set_mem_tabName) {
	  descrSetMemberMap.foreach(kv=>if(!colList.contains(kv._1)) {outputDF = outputDF.withColumn(kv._1,lit(kv._2))})
	}
	  outputDF
  }

  def addFields(inputDF: DataFrame, fieldNames: String): DataFrame = {
    val fieldArr = fieldNames.split(",")
    var outputDF = inputDF
    for(fld<-fieldArr){
      outputDF=outputDF.withColumn(fld,lit(null))
    }
    outputDF
  }

  def deleteDbRecsForInput(inputDF: DataFrame, dbConfig: DbConfig, deleteSql: String) = {
    inputDF.coalesce(1).rdd.mapPartitions((r) => Iterator(r)).foreach {
      batch =>       val dbc = java.sql.DriverManager.getConnection(dbConfig.jdbcUrl,dbConfig.user,dbConfig.passwd);
        val stmt = deleteSql
        val pst = dbc.prepareStatement(stmt)
        batch.grouped(1000).foreach {
          case(session1) => session1 foreach {
            session => pst.setString(1, session.getString(1))
              pst.addBatch
          }
            pst.executeBatch
        }
        dbc.close }

  }

  def clearDbTempTables(dbConfig: DbConfig, tabName: String) = {
    val sql = "truncate table " + tabName
    val dbc = java.sql.DriverManager.getConnection(dbConfig.jdbcUrl,dbConfig.user,dbConfig.passwd)
    val stmt = dbc.createStatement
    stmt.executeUpdate(sql)
  }

  def callDbProc(dbConfig: DbConfig, procName: String) = {
    val proc_name =" {call " + procName + "()}";
    val dbc = java.sql.DriverManager.getConnection(dbConfig.jdbcUrl,dbConfig.user,dbConfig.passwd)
    val callableStatement = dbc.prepareCall(proc_name);
    callableStatement.execute();
    dbc.close;
  }
}
