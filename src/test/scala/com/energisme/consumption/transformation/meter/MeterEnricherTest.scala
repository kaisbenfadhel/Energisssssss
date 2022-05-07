package com.energisme.consumption.transformation.meter

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.{CleanerService, EnricherService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class MeterEnricherTest extends FunSpec with Matchers with SparkSessionUtils {

  it("should map (explode) columns  meter_sensor in function mapColumns ") {
    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val preprocessingMeterleaner = new cleanerServiceHelper(CleanerService("preprocessingmeter"))

    val metercleanerDataFrame = preprocessingMeterleaner.clean(dataFrameMeter)

    EnricherService("enrichermeter").mapColumns(metercleanerDataFrame).collect should have length 16
  }

}
