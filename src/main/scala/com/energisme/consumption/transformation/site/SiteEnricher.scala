package com.energisme.consumption.transformation.site

import com.energisme.consumption.interfaces.EnricherService
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


class SiteEnricher extends EnricherService {
  /**
    * fusionned three DataFrame
    *
    * @param df Input DataFrame (Site.json)
    * @return
    */
  override def fusionDataFrame(df: DataFrame): DataFrame = {
    createMappedDataFrame(df, "entry_position", "entry_meter").unionByName(createMappedDataFrame(df: DataFrame, "exit_position", "exit_meter")).unionByName(createMappedDataFrame(df: DataFrame, "middle_position", "middle_meter"))
  }

  /**
    * Create DataFrame mapped and  Add "position" column
    *
    * @param df           Input DataFrame
    * @param valueColumn  value of position
    * @param mappedColumn coloumn mapped
    * @return
    */


  def createMappedDataFrame(df: DataFrame, valueColumn: String, mappedColumn: String): DataFrame = {
    addColumnPosition(mapColumns(df, mappedColumn), valueColumn)
  }

  /**
    * This function : Add position column
    *
    * @param df          Input DataFrame
    * @param valueColumn : value of position (exit_position,middle_position or entry_position )
    * @return
    */

  def addColumnPosition(df: DataFrame, valueColumn: String): DataFrame = {
    df.withColumn("Position", lit(valueColumn))
  }

  /**
    * Create  dataframe where we  map(explode) column
    *
    * @param df Input DataFrame
    * @param mappedColumn column exploded
    * @return Mapped DataFrame
    */
  override def mapColumns(df: DataFrame, mappedColumn: String): DataFrame = {

    persistData(df).select(col("client_code"), col("code_site"), explode(col(mappedColumn)).as("code_meter"))
  }

  override def mapColumns(df: DataFrame): DataFrame = {

    df
  }

  /**
    * Serializes the DataFrame objects in memory and on disk when space is not available
    *
    * @param df Input DataFrame
    * @return
    */
  override def persistData(df: DataFrame): DataFrame = {
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def withSensorData(sensorData: DataFrame, meterData: DataFrame, siteData: DataFrame, rawData: DataFrame): DataFrame = {
    sensorData
  }

}
