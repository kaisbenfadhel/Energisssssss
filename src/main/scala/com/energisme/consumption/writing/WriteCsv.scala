package com.energisme.consumption.writing

import com.energisme.consumption.interfaces.WriterService
import org.apache.spark.sql.{DataFrame, SaveMode}


class WriteCsv extends WriterService {
  /**
    * This function write  partitionned CSV file
    *
    * @param df   :DataFrame Source
    * @param cols :one or multiple columns while writing to disk
    * @param path :target path
    *
    */

  override def writeCsv(df: DataFrame, cols: List[String], path: String) = {
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy(cols: _*)
      .option("header", "true")
      .csv(path)
  }

}
