package com.energisme.consumption.transformation.allData

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.setting.Settings
import com.energisme.consumption.interfaces.{CleanerService, EnricherService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.preprocessing.sensor.cleanerSensorServiceHelper
import com.energisme.consumption.reading.FileReader
import com.energisme.consumption.transformation.meter.EnricherMeterServiceHelper
import com.energisme.consumption.transformation.site.EnricherSiteServiceHelper
import org.scalatest.{FunSpec, Matchers}


class AllEnricherTest extends FunSpec with Matchers with SparkSessionUtils {

  it("should join 4 DataFrame meter ,sensor,raw_data and site with function withSensorData ") {
    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val preprocessingMeterleaner = new cleanerServiceHelper(CleanerService("preprocessingmeter"))
    val metercleanerDataFrame = preprocessingMeterleaner.clean(dataFrameMeter)
    val meterEnricher = EnricherMeterServiceHelper(EnricherService("enrichermeter")).enrich(metercleanerDataFrame)

    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)
    val preprocessingSiteleaner = new cleanerServiceHelper(CleanerService("preprocessingsite"))
    val sitecleanerDataFrame = preprocessingSiteleaner.clean(dataFrameSite)
    val siteEnricher = EnricherSiteServiceHelper(EnricherService("enrichersite")).enrich(sitecleanerDataFrame)

    val preprocessingSensorCleaner = new cleanerSensorServiceHelper(CleanerService("preprocessingsensor"))
    val filePathSensor = "src/test/resources/data/sensor.json"
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)
    val sensorCleanerDataFrame = preprocessingSensorCleaner.clean(dataFrameSensor)

    val rawDataDf = FileReader.readCsvDataFrame("src/test/resources/data/raw_data.csv")

    val enrichedAllDf = EnricherAllServiceHelper(EnricherService("enricherall")).enrich(siteEnricher, meterEnricher, sensorCleanerDataFrame, rawDataDf)
    enrichedAllDf.collect should have length 18
  }


}
