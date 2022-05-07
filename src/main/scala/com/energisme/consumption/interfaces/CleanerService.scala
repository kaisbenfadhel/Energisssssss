package com.energisme.consumption.interfaces

import com.energisme.consumption.preprocessing.meter.MeterCleaner
import com.energisme.consumption.preprocessing.site.SiteCleaner
import com.energisme.consumption.preprocessing.sensor.SensorCleaner
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CleanerService {

  def replaceNull(df: DataFrame,amended: String): DataFrame

  def replaceAllNull(df: DataFrame): DataFrame

  def columnstranslatorMap : Map[String, String]

  def RenameColumns(df: DataFrame): DataFrame

  def replaceNull(df: DataFrame,amended: String,columnToModify: String): DataFrame

  def convertToTimeStamp(df: DataFrame)(implicit spark: SparkSession): DataFrame



}
object CleanerService {
  def apply(cleanerType: String): CleanerService = {
    cleanerType match {
      case "preprocessingsite" => new SiteCleaner()
      case "preprocessingmeter" => new MeterCleaner()
      case "preprocessingsensor" => new SensorCleaner()
    }
  }
}
