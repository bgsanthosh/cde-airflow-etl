package org.cloudera.cde.app

import org.apache.spark.sql.SparkSession

package object SparkSetup {

  final val sparkApp = "InsuranceCDEApp"
  // create the spark session
  val spark = SparkSession.builder
    .appName(sparkApp)
    .enableHiveSupport().getOrCreate()

  //set sparkContext object
  val sc = spark.sparkContext

  // set sqlContext object
  val sqlContext = spark.sqlContext

  println("spark warehouse dir:" + spark.conf.get("spark.sql.warehouse.dir"))
  //println("hive warehouse dir:" + spark.conf.get("hive.metastore.warehouse.dir"))
  // commenting below lines because on DE cluster this config is not available and crashes spark app !!!
  //println("kubernetes dir:" + spark.conf.get("spark.kubernetes.container.image"))

}