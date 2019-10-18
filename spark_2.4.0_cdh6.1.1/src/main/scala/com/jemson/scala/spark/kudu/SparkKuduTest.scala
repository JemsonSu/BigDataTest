package com.jemson.scala.spark.kudu

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext, SparkSession}

/**
 * scala版单纯用spark2.x读取kudu数据
 */
object SparkKuduTest {
  def main(args: Array[String]): Unit = {

    //前面四行是spark2.X官方推荐这样写的
    val builder: SparkSession.Builder = SparkSession.builder()
    builder.appName("SparkKuduTest")
    builder.master("local")
    val spark: SparkSession = builder.getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("ERROR")


    val read: DataFrameReader = spark.read

    val sqlContext: SQLContext = spark.sqlContext


    val kuduMaster = "c21"
    val tableName = "kudu_users"

    val kuduContext = new KuduContext(kuduMaster, sparkContext)


    val rows: Seq[String] = Seq("id","name")
    val rdd: RDD[Row] = kuduContext.kuduRDD(sparkContext, tableName, rows)

    rdd.foreach(println)











  }
}
