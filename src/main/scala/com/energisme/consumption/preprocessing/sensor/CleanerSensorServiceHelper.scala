package com.energisme.consumption.preprocessing.sensor

import com.energisme.consumption.interfaces.CleanerService
import org.apache.spark.sql.{DataFrame, SparkSession}

case class cleanerSensorServiceHelper(generalCleaner: CleanerService) {

  /**
    * Function clean DataFrame
    *
    * @param df Input DataFrame
    * @return
    */
  def clean(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.transform(generalCleaner.RenameColumns)
      .transform(generalCleaner.replaceAllNull)
        .transform(generalCleaner.convertToTimeStamp)

  }
}