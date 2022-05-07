package com.energisme.consumption.reading

import com.energisme.consumption.SparkSessionUtils
import org.scalatest.{FunSpec, Matchers}

class FileReaderTest extends FunSpec with Matchers with SparkSessionUtils {

  it("Should correctly load DataFrame Given valid file data") {
    // GIVEN
    val filePathSensor = "src/test/resources/data/sensor.json"
    val filePathMeter = "src/test/resources/data/meter.json"
    val filePathSite = "src/test/resources/data/site.json"
    val filePathRawData = "src/test/resources/data/raw_data.csv"

    // WHEN
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)
    val dataFrameMeter = FileReader.readJsonDataFrame(filePathMeter)
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)
    val dataFrameRawData = FileReader.readCsvDataFrame(filePathRawData)

    // THEN
    dataFrameSensor.columns should contain only("client.code", "code", "sensor.id", "sensor.installation", "sensor.pulse_weight", "sensor.uninstallation", "tag")
    dataFrameMeter.columns should contain only("client.code", "code", "meter.fluid", "meter.sensor")
    dataFrameSite.columns should contain only("client.code", "code", "entry.meter", "exit.meter", "middle.meter")
    dataFrameRawData.columns should contain only("year", "sensor_id", "timestamp", "value")


  }

}
