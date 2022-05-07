package com.energisme.consumption.interfaces

import org.apache.spark.sql.DataFrame
import com.energisme.consumption.writing.WriteCsv
trait WriterService {

  def writeCsv(df: DataFrame,cols: List[String],path : String)

}


object WriterService {
  def apply(): WriterService = {
    new WriteCsv()
  }
}




