package com.energisme.consumption.preprocessing.meter

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.CleanerService
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}


class MeterCleanerTest extends FunSpec with Matchers with SparkSessionUtils {


  it("should rename columns of the DataFrame Meter ") {

    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)

    CleanerService("preprocessingmeter").RenameColumns(dataFrameMeter).columns should contain only("client_code", "code_meter", "meter_fluid", "meter_sensor")

  }


  it("should Replace   Null with array[] with Two parameter ") {

    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val meterDfRenamed = CleanerService("preprocessingmeter").RenameColumns(dataFrameMeter)


    val meterDfRenamedNotNull = CleanerService("preprocessingmeter").replaceNull(meterDfRenamed, "meter_sensor")

    val meterAsList = meterDfRenamedNotNull
      .filter(meterDfRenamedNotNull("code_meter")
        .equalTo("meter_6"))
      .select("meter_sensor")
      .collectAsList()

    val meterSensor = meterAsList.get(0)(0)

    assert(Array().deep == (meterSensor))

  }
  it("should Replace   Null with array[] with One parameter  ") {

    val filePathMeter = "src/test/resources/data/meter.json"
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val meterDfRenamed = CleanerService("preprocessingmeter").RenameColumns(dataFrameMeter)


    val meterDfRenamedNotNull = CleanerService("preprocessingmeter").replaceAllNull(meterDfRenamed)

    val meterAsList = meterDfRenamedNotNull
      .filter(meterDfRenamedNotNull("code_meter")
        .equalTo("meter_6"))
      .select("meter_sensor")
      .collectAsList()

    val meterSosor = meterAsList.get(0)(0)

    assert(Array().deep == (meterSosor))

  }


}
