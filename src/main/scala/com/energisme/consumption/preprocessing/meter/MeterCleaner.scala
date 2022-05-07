package com.energisme.consumption.preprocessing.meter

import com.energisme.consumption.interfaces.CleanerService
import org.apache.spark.sql.functions.{array, coalesce, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

class MeterCleaner extends CleanerService {

  /**
    * Replace   Null with array[]
    *
    * @param df      Input DataFrame
    * @param amended Column amended
    * @return DataFrame clean
    */
  override def replaceNull(df: DataFrame, amended: String): DataFrame = {
    df.withColumn(amended, coalesce(col(amended), array()))
  }

  /**
    * Replace   Null with array[]
    *
    * @param df Input DataFrame
    * @return DataFrame clear null
    */
  override def replaceAllNull(df: DataFrame): DataFrame = {
    replaceNull((replaceNull(df, "meter_sensor")), "meter_sensor")
  }

  override def replaceNull(df: DataFrame, amended: String, columnToModify: String): DataFrame = {
    df
  }

  /**
    * Map String To another
    *
    * @return
    */

  override def columnstranslatorMap: Map[String, String] = {
    Map(
      "client.code" -> "client_code",
      "code" -> "code_meter",
      "meter.fluid" -> "meter_fluid",
      "meter.sensor" -> "meter_sensor"
    )
  }

  /**
    * Rename Column with DataFrame Meter
    *
    * @param df
    * @return
    */
  override def RenameColumns(df: DataFrame): DataFrame = {
    columnstranslatorMap.foldLeft(df) {
      case (df, (oldName, newName)) => df.withColumnRenamed(oldName, newName)
    }


  }

  override def convertToTimeStamp(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df
  }

}
