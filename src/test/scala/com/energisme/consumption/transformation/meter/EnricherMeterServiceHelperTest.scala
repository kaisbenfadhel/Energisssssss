package com.energisme.consumption.transformation.meter

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.{CleanerService, EnricherService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.reading.FileReader
import com.energisme.consumption.transformation.site.EnricherSiteServiceHelper
import org.scalatest.{FunSpec, Matchers}

class EnricherMeterServiceHelperTest extends FunSpec with Matchers with SparkSessionUtils {

  it("should enrich meter   DataFrame with function enrich ") {
    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val preprocessingMeterleaner = new cleanerServiceHelper(CleanerService("preprocessingmeter"))

    val metercleanerDataFrame = preprocessingMeterleaner.clean(dataFrameMeter)
    val meterEnricher = EnricherMeterServiceHelper(EnricherService("enrichermeter")).enrich(metercleanerDataFrame)
    meterEnricher.collect should have length 16
  }

}
