package com.energisme.consumption

import org.apache.spark.sql.SparkSession

trait SparkSessionUtils {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "local[*]")
    .config("spark.eventLog.enabled","true")
    .config("spark.history.fs.logDirectory","file:///C:/tmp/spark-events")
    .config("spark.eventLog.dir","file:///C:/tmp/spark-events")
    .appName("Energisme")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
}
