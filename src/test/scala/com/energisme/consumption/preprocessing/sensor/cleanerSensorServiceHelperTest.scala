package com.energisme.consumption.preprocessing.sensor

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.CleanerService
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class cleanerSensorServiceHelperTest extends FunSpec with Matchers with SparkSessionUtils {

  it("should clean DataFrame in function clean ") {
    val preprocessingSensorCleaner = new cleanerSensorServiceHelper(CleanerService("preprocessingsensor"))
    val filePathSensor = "src/test/resources/data/sensor.json"
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)

    val sensorCleanerDataFrame = preprocessingSensorCleaner.clean(dataFrameSensor)


    val sensorTagAsList = sensorCleanerDataFrame
      .filter(sensorCleanerDataFrame("code_sensor")
        .equalTo("sensor_3"))
      .select("tag")
      .collectAsList()
    val sensorInstAsList = sensorCleanerDataFrame
      .filter(sensorCleanerDataFrame("code_sensor")
        .equalTo("sensor_3"))
      .select("sensor_installation").collectAsList()

    val sensorUninAsList = sensorCleanerDataFrame
      .filter(sensorCleanerDataFrame("code_sensor")
        .equalTo("sensor_3"))
      .select("sensor_uninstallation").collectAsList()

    val typeSensIns = sensorCleanerDataFrame.schema("sensor_installation").dataType
    val typeSensunins = sensorCleanerDataFrame.schema("sensor_uninstallation").dataType


    val tag = sensorTagAsList.get(0)(0)
    val sensor_installation = sensorInstAsList.get(0)(0).toString
    val sensor_uninstallation = sensorUninAsList.get(0)(0).toString


    assert("TimestampType" == (typeSensIns.toString) && "TimestampType" == (typeSensunins.toString) && ("unknown" == (tag)) && ("1970-01-01 00:00:00.0" == sensor_installation) && ("2600-01-01 00:00:00.0" == sensor_uninstallation))

  }


}
