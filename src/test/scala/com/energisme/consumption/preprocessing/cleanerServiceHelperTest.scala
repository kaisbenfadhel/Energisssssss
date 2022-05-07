package com.energisme.consumption.preprocessing

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.CleanerService
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class cleanerServiceHelperTest extends FunSpec with Matchers with SparkSessionUtils{

  it("should clean DataFrame in function clean ") {
    val preprocessingSiteleaner = new cleanerServiceHelper(CleanerService("preprocessingsite"))
    val preprocessingMeterleaner = new cleanerServiceHelper(CleanerService("preprocessingmeter"))

    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)
    val siteDfRenamed = CleanerService("preprocessingsite").RenameColumns(dataFrameSite)
    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val meterDfRenamed = CleanerService("preprocessingmeter").RenameColumns(dataFrameMeter)

    val sitecleanerDataFrame = preprocessingSiteleaner.clean(siteDfRenamed)
    val metercleanerDataFrame = preprocessingMeterleaner.clean(meterDfRenamed)


    val siteMiddleAsList = sitecleanerDataFrame
      .filter(sitecleanerDataFrame("code_site")
        .equalTo("site_1"))
      .select("middle_meter")
      .collectAsList()
    val siteExitAsList = sitecleanerDataFrame
      .filter(sitecleanerDataFrame("code_site")
        .equalTo("site_2"))
      .select("exit_meter")
      .collectAsList()
    val meterAsList = metercleanerDataFrame
      .filter(metercleanerDataFrame("code_meter")
        .equalTo("meter_6"))
      .select("meter_sensor")
      .collectAsList()


    val middlemeter = siteMiddleAsList.get(0)(0)
    val exitemeter = siteExitAsList.get(0)(0)
    val meterSensor = meterAsList.get(0)(0)
    assert((Array().deep == (middlemeter) )     && (Array().deep == (exitemeter))&&( Array().deep == (meterSensor)))
  }

}
