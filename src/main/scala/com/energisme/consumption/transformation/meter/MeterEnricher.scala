package com.energisme.consumption.transformation.meter

import com.energisme.consumption.interfaces.EnricherService
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.storage.StorageLevel

class MeterEnricher extends EnricherService{



  def mapColumns(df: DataFrame,mappedColumn : String): DataFrame ={
    df
  }

  override def fusionDataFrame(df: DataFrame ) : DataFrame ={
    df
  }

  /**
    * Create  dataframe where we  map(explode) column
    *
    * @param df           Input DataFrame Meter
    * @return Mapped DataFrame
    */
  override def mapColumns(df: DataFrame): DataFrame ={

    persistData(df).select(col("client_code"),col("code_meter"),col("meter_fluid"),explode(col("meter_sensor")).as("code_sensor"))
  }
  /**
    * Serializes the DataFrame objects in memory and on disk when space is not available
    *
    * @param df Input DataFrame
    * @return
    */
  override def persistData(df: DataFrame) : DataFrame ={
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }
  override def withSensorData(sensorData: DataFrame,meterData: DataFrame,siteData: DataFrame,rawData: DataFrame): DataFrame ={
    sensorData
  }

}
