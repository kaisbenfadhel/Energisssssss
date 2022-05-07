package com.energisme.consumption.transformation.meter

import com.energisme.consumption.interfaces.EnricherService
import org.apache.spark.sql.DataFrame

case class EnricherMeterServiceHelper(generalCleaner: EnricherService) {

  /**
    * This Function enrich DataFrame meter ( map(explode) Column meter_sensor)
    *
    * @param df input DataFrame Meter
    * @return
    */
  def enrich(df: DataFrame): DataFrame = {
    df.transform(generalCleaner.mapColumns)


  }

}
