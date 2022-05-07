package com.energisme.consumption.transformation.site

import com.energisme.consumption.interfaces.EnricherService
import org.apache.spark.sql.DataFrame

case class EnricherSiteServiceHelper(generalCleaner: EnricherService) {
  /**
    * This Function enrich DataFrame Site ( map and Add column Position)
    *
    * @param df input DataFrame Site
    * @return
    */
  def enrich(df: DataFrame) = {
    df.transform(generalCleaner.fusionDataFrame)


  }
}
