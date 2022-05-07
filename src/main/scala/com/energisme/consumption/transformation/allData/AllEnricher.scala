package com.energisme.consumption.transformation.allData

import com.energisme.consumption.interfaces.EnricherService
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import com.energisme.consumption.enumeration.Columns

class AllEnricher extends EnricherService{

  /**
    *
    * Function Join 4 DataFrame
    * @param siteData DataFrame Site
    * @param meterData DataFrame Meter
    * @param sensorData DataFrame Sensor
    * @param rawData DataFrame raw_data
    * @return
    */
  override def withSensorData( siteData: DataFrame,meterData: DataFrame,sensorData: DataFrame,rawData: DataFrame): DataFrame = {
    persistData(withRowData(rawData,withOtherData(withOtherData(siteData,meterData,Columns.gb1.toString,Columns.cd_mtr.toString),sensorData,Columns.gb1.toString,
      Columns.cd_ssr.toString),Columns.ssr_id.toString,Columns.tstmp.toString,Columns.ssr_ins.toString,Columns.ssr_unins.toString)
        .withColumn(Columns.consumption.toString,col(Columns.ssr_pul_wei.toString) * col(Columns.vall.toString) ))

  }

  /**
    * Function join two DataFrme (Sensor (clearned and enriched) whith (meter and site))
    * @param JoinedSourcesDF Sensor (clearned and enriched)
    * @param df (meter and site)
    * @param value client_code
    * @param value1 code_sensor
    * @return
    */
  def withOtherData(JoinedSourcesDF: DataFrame,df: DataFrame,value :String,value1 :String): DataFrame = {

   persistData( (df.join(JoinedSourcesDF, Seq(value, value1) ,"inner")))
  }

  /**
    *
    * @param JoinedSourcesDF   sensor meter site joined
    * @param df  rawdata
    * @param value  sensor_id
    * @param value1 sensor_uninstallation
    * @param value2 timestamp
    * @param value3 sensor_installation
    * @return
    */
  def withRowData(JoinedSourcesDF: DataFrame,df: DataFrame,value :String,value1 :String,value2 :String,value3 :String): DataFrame = {

    persistData( (df.join(JoinedSourcesDF,df(value) ===  JoinedSourcesDF(value) and(df(value2)< JoinedSourcesDF(value1)) and(df(value3)>JoinedSourcesDF(value1)) ,"inner")))
  }

  /**
    * Serializes the DataFrame objects in memory and on disk when space is not available
    *
    * @param df Input DataFrame
    * @return
    */
   def persistData(df: DataFrame) : DataFrame ={
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }
  def fusionDataFrame(df: DataFrame ) : DataFrame={df}

  def mapColumns(df: DataFrame,mappedColumn : String): DataFrame={df}
  def mapColumns(df: DataFrame): DataFrame={df}


}
