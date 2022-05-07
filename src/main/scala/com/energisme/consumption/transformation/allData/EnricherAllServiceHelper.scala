package com.energisme.consumption.transformation.allData
import org.apache.spark.sql.{DataFrame}
import com.energisme.consumption.interfaces.EnricherService

  case class EnricherAllServiceHelper (generalCleaner: EnricherService) {
    def enrich(df: DataFrame,df1: DataFrame,df2: DataFrame,df3: DataFrame) : DataFrame = {
      generalCleaner.withSensorData(df,df1,df2,df3)


    }




}