package com.energisme.consumption.preprocessing.sensor

import com.energisme.consumption.interfaces.CleanerService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}



class SensorCleaner extends CleanerService {



  /**
    * Map String To another
    *
    * @return
    */

  override def columnstranslatorMap: Map[String, String] = {
    Map(
      "client.code" -> "client_code",
      "code" -> "code_sensor",
      "sensor.id" -> "sensor_id",
      "sensor.pulse_weight" -> "sensor_pulse_weight",
      "sensor.installation" -> "sensor_installation",
      "sensor.uninstallation" -> "sensor_uninstallation",
      "tag" -> "tag"
    )
  }

  /**
    * Replace   Null with array[]
    *
    * @param df Input DataFrame
    * @return DataFrame clear null
    */
  override def replaceNull(df: DataFrame, amended: String, columnToModify: String) : DataFrame = {
    df.na.fill(amended, Array(columnToModify))
  }

  override def replaceNull(df: DataFrame, amended: String) : DataFrame = {
    df
  }

  /**
    *Rename Column of DataFrame Sensor
    *
    * @param df
    * @return DataFrame with column renamed
    */
  override def RenameColumns(df: DataFrame): DataFrame = {
    columnstranslatorMap.foldLeft(df) {
      case (df, (oldName, newName)) => df.withColumnRenamed(oldName, newName)
    }
  }

  /**
    *  Replace   Null with String of Many Column
    *
    * @param df Input DataFrame
    * @return
    */
  override def replaceAllNull(df: DataFrame): DataFrame = {
    replaceNull(replaceNull(replaceNull(df, "unknown", "tag"), "1970-01-01T00:00:00", "sensor_installation"), "2600-01-01T00:00:00", "sensor_uninstallation")
  }

  /**
//    * Convert Column String To timestamp
    * @param df
    * @param spark implicit SparkSession
    * @return
    */
  override  def convertToTimeStamp(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.select($"client_code",$"code_sensor",$"sensor_id",to_timestamp($"sensor_installation").as("sensor_installation"),$"sensor_pulse_weight",
      to_timestamp($"sensor_uninstallation").as("sensor_uninstallation"),$"tag"   )
  }




}

