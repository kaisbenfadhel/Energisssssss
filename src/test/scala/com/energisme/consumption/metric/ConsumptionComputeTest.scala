package com.energisme.consumption.metric

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.enumeration.Columns
import com.energisme.consumption.interfaces.{CleanerService, ComputeCalculationService, EnricherService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.preprocessing.sensor.cleanerSensorServiceHelper
import com.energisme.consumption.reading.FileReader
import com.energisme.consumption.transformation.allData.EnricherAllServiceHelper
import com.energisme.consumption.transformation.meter.EnricherMeterServiceHelper
import com.energisme.consumption.transformation.site.EnricherSiteServiceHelper
import org.scalatest.{FunSpec, Matchers}

class ConsumptionComputeTest extends FunSpec with Matchers with SparkSessionUtils {
  it("should Compute Consumption by Flluid By MeterPosition By Site ") {

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

    val allEnricher = EnricherAllServiceHelper(EnricherService("enricherall")).enrich(siteEnricher, meterEnricher, sensorCleanerDataFrame, rawDataDf)
    val ByFluidByMeterPositionBySiteDf = ComputeCalculationService().byFlluidByMeterPositionBySite(allEnricher, Columns.gb1.toString, Columns.gb2.toString, Columns.gb3.toString, Columns.gb4.toString, Columns.consumption.toString, Columns.aliasByFluidByMeterPositionBySite.toString)
    ByFluidByMeterPositionBySiteDf.columns should contain only("client_code", "position", "meter_fluid", "code_site", "ByFluidByMeterPositionBySite")
  }

  it("should Compute Consumption by Flluid By MeterPosition For N Site ") {

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

    val allEnricher = EnricherAllServiceHelper(EnricherService("enricherall")).enrich(siteEnricher, meterEnricher, sensorCleanerDataFrame, rawDataDf)
    val ByFluidByMeterPositionForNSiteDf = ComputeCalculationService().byFlluidByMeterPositionForNSite(allEnricher, Columns.gb1.toString, Columns.gb2.toString, Columns.gb3.toString, Columns.consumption.toString, Columns.aliasByFluidByMeterPositionForNSite.toString)

    ByFluidByMeterPositionForNSiteDf.columns should contain only("client_code", "position", "meter_fluid", "ByFluidByMeterPositionForNSite")
  }
  it("should Compute Consumption by by Tag By Site ") {

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

    val allEnricher = EnricherAllServiceHelper(EnricherService("enricherall")).enrich(siteEnricher, meterEnricher, sensorCleanerDataFrame, rawDataDf)
    val ByFluidbyTagBySite = ComputeCalculationService().byTagBySite(allEnricher, Columns.gb1.toString, Columns.gb4.toString, Columns.gb5.toString, Columns.consumption.toString, Columns.aliasByTagBySite.toString, Columns.column.toString, Columns.valu.toString)

    ByFluidbyTagBySite.columns should contain only("client_code", "code_site", "tag", "ByTagBySite")
  }
  it("should Compute Consumption by Tag For N Site ") {

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

    val allEnricher = EnricherAllServiceHelper(EnricherService("enricherall")).enrich(siteEnricher, meterEnricher, sensorCleanerDataFrame, rawDataDf)
    val byTagForNSite = ComputeCalculationService().byTagForNSite(allEnricher, Columns.gb1.toString, Columns.gb5.toString, Columns.consumption.toString, Columns.aliasByTagByNSite.toString, Columns.column.toString, Columns.valu.toString)

    byTagForNSite.columns should contain only("client_code", "tag", "ByTagByNSite")
  }

}
