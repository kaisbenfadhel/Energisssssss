package com.energisme.consumption.interfaces
import com.energisme.consumption.metrics.ConsumptionCompute
import org.apache.spark.sql.{Column, DataFrame}

trait ComputeCalculationService {

  def byFlluidByMeterPositionBySite(df: DataFrame, gb1 :String,gb2 :String,gb3 :String,gb4 :String, consumption: String, alias: String): DataFrame

  def byFlluidByMeterPositionForNSite(df: DataFrame, gb1 :String,gb2 :String,gb3 :String, consumption: String, alias: String): DataFrame

  def byTagBySite(df: DataFrame, gb1 :String,gb2 :String,gb3 :String, consumption: String, alias: String,column : String,value :String): DataFrame

  def byTagForNSite(df: DataFrame, gb1 :String,gb2 :String, consumption: String, alias: String,column : String,value :String): DataFrame

}


object ComputeCalculationService {
  def apply(): ComputeCalculationService = {

       new ConsumptionCompute()



  }
}