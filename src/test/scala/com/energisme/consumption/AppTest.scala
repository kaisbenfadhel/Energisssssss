package com.energisme.consumption

import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class AppTest extends FunSpec with Matchers with SparkSessionUtils {

  it("Should write consumption in correct repository Given file data calculated by Target By MeterPosition by Site") {
    // GIVEN
    val sensorpath = "src/test/resources/data/sensor.json"
    val meterpath = "src/test/resources/data/meter.json"
    val sitepath = "src/test/resources/data/site.json"
    val rawpath = "src/test/resources/data/raw_data.csv"


    // WHEN
    App.main(Array(sensorpath,sitepath,meterpath,rawpath))
    // THEN
    val resultByFluidByMeterpositionBySite = spark.read.csv("src/test/resources/consumption/by_fluid_by_meter_position_by_site")
    resultByFluidByMeterpositionBySite.collect() should have length 24
  }

 /* it("Should write invalid transactions in correct repository Given file data with invalid transactions") {
    // GIVEN
    val filePath = "src/test/resources/data/simple.csv"
    val targetPath = "src/test/resources/processed"

    // WHEN
    App.main(Array(filePath, targetPath))

    // THEN
    val resultDataFrame = spark.read.csv(s"$targetPath/invalid/")
    resultDataFrame.collect() should have length 2
  }*/

}
