package com.energisme.consumption.preprocessing.site

import com.energisme.consumption.interfaces.CleanerService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class SiteCleaner extends CleanerService {


  /**
    * Add Column amended after Replace   Null with array[]
    *
    * @param df      Input DataFrame
    * @param amended Column amended
    * @return DataFrame clean
    */
  override def replaceNull(df: DataFrame, amended: String): DataFrame = {
    df.withColumn(amended, coalesce(col(amended), array()))
  }

  override def replaceNull(df: DataFrame, amended: String, columnToModify: String): DataFrame = {
    df
  }


  /**
    * Replace   Null with array[]
    *
    * @param df Input DataFrame
    * @return DataFrame clear null
    */


  override def replaceAllNull(df: DataFrame): DataFrame = {
    replaceNull((replaceNull(df, "middle_meter")), "exit_meter").as("meter")
  }

  /**
    * Map String To another
    *
    * @return
    * //    */

  override def columnstranslatorMap: Map[String, String] = {
    Map(
      "client.code" -> "client_code",
      "code" -> "code_site",
      "entry.meter" -> "entry_meter",
      "exit.meter" -> "exit_meter",
      "middle.meter" -> "middle_meter"
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
