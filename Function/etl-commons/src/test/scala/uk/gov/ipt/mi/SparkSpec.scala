package uk.gov..mi

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSpec extends BeforeAndAfterAll {

  this: Suite =>

  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[*]")

  override def beforeAll {
    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }
    spark = SparkSession.builder.config(conf).getOrCreate()
    sqlContext = spark.sqlContext
  }

  override def afterAll {
    if (spark != null) {
      spark.stop()
    }
  }

  def sparkConfig: Map[String, String] = Map.empty
}
