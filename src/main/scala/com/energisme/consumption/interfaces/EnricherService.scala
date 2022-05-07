package com.energisme.consumption.interfaces
import com.energisme.consumption.transformation.site.SiteEnricher
import com.energisme.consumption.transformation.meter.MeterEnricher
import com.energisme.consumption.transformation.allData.AllEnricher
import org.apache.spark.sql.{DataFrame, SparkSession}
trait EnricherService {

  def fusionDataFrame(df: DataFrame ) : DataFrame
  def mapColumns(df: DataFrame,mappedColumn : String): DataFrame
  def mapColumns(df: DataFrame): DataFrame
  def persistData(df: DataFrame) : DataFrame
  def withSensorData(sensorData: DataFrame,meterData: DataFrame,siteData: DataFrame,rawData: DataFrame): DataFrame
}



object EnricherService {
  def apply(enricherType: String): EnricherService = {
    enricherType match {
      case "enrichersite" => new SiteEnricher()
      case "enrichermeter" => new MeterEnricher()
      case "enricherall" => new AllEnricher()

    }
  }
}