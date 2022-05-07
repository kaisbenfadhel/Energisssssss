package com.energisme.consumption.preprocessing.sensor

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.CleanerService
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class SensorCleanerTest extends FunSpec with Matchers with SparkSessionUtils {

  it("should rename columns of the DataFrame Sensor ") {

    val filePathSensor = "src/test/resources/data/sensor.json"
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)

    CleanerService("preprocessingsensor").RenameColumns(dataFrameSensor).columns should contain only("client_code", "code_sensor", "sensor_id", "sensor_pulse_weight", "sensor_installation", "sensor_uninstallation", "tag")


  }

  it("should Replace   Null with unknown with Three parameter in function replaceNull ") {

    val filePathSensor = "src/test/resources/data/sensor.json"
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)
    val sensorDfRenamed = CleanerService("preprocessingsensor").RenameColumns(dataFrameSensor)


    val sensorDfRenamedNotNull = CleanerService("preprocessingsensor").replaceNull(sensorDfRenamed, "unknown", "tag")

    val sensorAsList = sensorDfRenamedNotNull
      .filter(sensorDfRenamedNotNull("code_sensor")
        .equalTo("sensor_3"))
      .select("tag")
      .collectAsList()

    val tag = sensorAsList.get(0)(0)

    assert("unknown" == (tag))

  }

  it("should Replace   All Null with unknown in column tag , 1970-01-01T00:00:00 in column sensor_installation and 2600-01-01T00:00:00 in column sensor_uninstallation   on function replaceAllNull ") {
    val filePathSensor = "src/test/resources/data/sensor.json"
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)
    val sensorDfRenamed = CleanerService("preprocessingsensor").RenameColumns(dataFrameSensor)


    val sensorDfRenamedNotNull = CleanerService("preprocessingsensor").replaceAllNull(sensorDfRenamed)


    val sensorTagAsList = sensorDfRenamedNotNull
      .filter(sensorDfRenamedNotNull("code_sensor")
        .equalTo("sensor_3"))
      .select("tag")
      .collectAsList()
    val sensorInstAsList = sensorDfRenamedNotNull
      .filter(sensorDfRenamedNotNull("code_sensor")
        .equalTo("sensor_3"))
      .select("sensor_installation")
      .collectAsList()
    val sensorUninAsList = sensorDfRenamedNotNull
      .filter(sensorDfRenamedNotNull("code_sensor")
        .equalTo("sensor_3"))
      .select("sensor_uninstallation")
      .collectAsList()


    val tag = sensorTagAsList.get(0)(0)
    val sensor_installation = sensorInstAsList.get(0)(0)
    val sensor_uninstallation = sensorUninAsList.get(0)(0)

    assert(("unknown" == (tag)) && ("1970-01-01T00:00:00" == sensorInstAsList.get(0)(0)) && ("2600-01-01T00:00:00" == sensorUninAsList.get(0)(0)))


  }
  it("should convert column(sensor_installation,sensor_uninstallation) to timestamp with function convertToTimeStamp ") {
    val filePathSensor = "src/test/resources/data/sensor.json"
    val dataFrameSensor = FileReader.readJsonDataFrame(filePathSensor)
    val sensorDfRenamed = CleanerService("preprocessingsensor").RenameColumns(dataFrameSensor)

    val sensorDfRenamedNotNull = CleanerService("preprocessingsensor").replaceAllNull(sensorDfRenamed)
    val sensorConvertedDf = CleanerService("preprocessingsensor").convertToTimeStamp(sensorDfRenamedNotNull)

    val typeSensIns = sensorConvertedDf.schema("sensor_installation").dataType
    val typeSensunins = sensorConvertedDf.schema("sensor_uninstallation").dataType


    assert("TimestampType" == (typeSensIns.toString) && "TimestampType" == (typeSensunins.toString))


  }
}
