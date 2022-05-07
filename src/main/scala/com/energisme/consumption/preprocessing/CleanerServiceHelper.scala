package com.energisme.consumption.preprocessing

import com.energisme.consumption.interfaces.CleanerService
import org.apache.spark.sql.DataFrame

case class cleanerServiceHelper(generalCleaner: CleanerService) {
  /**
    * Function clean DataFrame
    *
    * @param df Input DataFrame
    * @return
    */
  def clean(df: DataFrame): DataFrame = {
    df.transform(generalCleaner.RenameColumns)
      .transform(generalCleaner.replaceAllNull)

  }
}