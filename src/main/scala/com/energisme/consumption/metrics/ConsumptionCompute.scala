package com.energisme.consumption.metrics

import com.energisme.consumption.interfaces.ComputeCalculationService
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



class ConsumptionCompute extends ComputeCalculationService {

  /**
    *Compute Consumption by Flluid By MeterPosition By Site
    *
    * @param df DataFrame joined cleared Enriched
    * @return
    */
  override def byFlluidByMeterPositionBySite(df: DataFrame, gb1: String, gb2: String, gb3: String, gb4: String, consumption: String, alias: String): DataFrame = {
    df.groupBy(gb1, gb2, gb3, gb4).agg(sum(consumption).as(alias))
  }

  
  
  
  /**
    *Compute Consumption by Flluid By MeterPosition For N Site
    *
    * @param df DataFrame joined cleared Enriched
    * @return DataFrame DataFrame Computed
    */
  override def byFlluidByMeterPositionForNSite(df: DataFrame, gb1: String, gb2: String, gb3: String, consumption: String, alias: String): DataFrame = {
    df.groupBy(gb1, gb2, gb3).agg(sum(consumption).as(alias))
  }

  /**
    *Compute Consumption by Tag By Site
    *
    * @param df DataFrame joined cleared Enriched
    * @return DataFrame Computed
    */
  
  override def byTagBySite(df: DataFrame, gb1: String, gb2: String, gb3: String, consumption: String, alias: String, column: String, value: String): DataFrame = {

    df.groupBy(gb1, gb2, gb3).agg(sum(consumption).as(alias)).where(col(column) notEqual value)
  }

  /**
    *Compute Consumption by Tag For N Site
    *
    * @param df  DataFrame joined cleared Enriched
    * @return DataFrame Computed
    */
  override def byTagForNSite(df: DataFrame, gb1: String, gb2: String, consumption: String, alias: String, column: String, value: String) = {
    df.groupBy(gb1, gb2).agg(sum(consumption).as(alias)).where(col(column) notEqual value)
  }

}
