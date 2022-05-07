package com.energisme.consumption.reading

import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReader {

 
 
/* /**
  *
  * @param sensorPath path of file
  * @param spark     implicit SparkSession
  * @return
  */*/
/*  def readSensorDataFrame(sensorPath: String)(implicit spark: SparkSession): DataFrame = {
  spark.read
    .options(Map("multiline" -> "true"))
    .json(sensorPath)
}*/

/**
  * Read Json File
  *
  * @param filePath path of file
  * @param spark implicit SparkSession
  * @return
  */
def readJsonDataFrame(filePath: String)(implicit spark: SparkSession): DataFrame = {
  spark.read
    .options(Map("multiline" -> "true", "inferSchema" -> "true"))
    .json(filePath)
}

/**
  * Read CSV file raw_data
  *
  * @param filePath path of file
  * @param spark val SparkSession: Nothing = null
  * @return
  */
def readCsvDataFrame(filePath: String)(implicit spark: SparkSession): DataFrame = {
  spark.read
    .options(Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ";"))
    .csv(filePath)
}

}
