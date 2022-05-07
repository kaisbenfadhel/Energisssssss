package com.energisme.consumption

import com.energisme.consumption.enumeration.Columns
import com.energisme.consumption.interfaces.{CleanerService, ComputeCalculationService, EnricherService, WriterService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.preprocessing.sensor.cleanerSensorServiceHelper
import com.energisme.consumption.reading.FileReader
import com.energisme.consumption.setting.Settings
import com.energisme.consumption.transformation.allData.EnricherAllServiceHelper
import com.energisme.consumption.transformation.meter.EnricherMeterServiceHelper
import com.energisme.consumption.transformation.site.EnricherSiteServiceHelper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object App {
  // private val logger = LoggerFactory.getLogger(getClass.getName)
  implicit val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "local[*]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.history.fs.logDirectory", "file:///C:/tmp/spark-events")
    .config("spark.eventLog.dir", "file:///C:/tmp/spark-events")
    .appName("Energisme")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")
  def main(args: Array[String]): Unit = {

    // logger.info("main method started")
    val conf: Config = ConfigFactory.load("properties.json")
    val settings: Settings = new Settings(conf)
    val sensorpath = args(0)
    val sitepath = args(1)
    val meterpath = args(2)
    val rawpath = args(3)

    // reading
    val sensorDf = FileReader.readJsonDataFrame(sensorpath)
    val siteDf = FileReader.readJsonDataFrame(sitepath)
    val meterDf = FileReader.readJsonDataFrame(meterpath)
    val rawDataDf = FileReader.readCsvDataFrame(rawpath)

    /*val sensorDf = FileReader.readJsonDataFrame(settings.sensorpath)
    val siteDf = FileReader.readJsonDataFrame(settings.sitepath)
    val meterDf = FileReader.readJsonDataFrame(settings.meterpath)
    val rawDataDf = FileReader.readCsvDataFrame(settings.rawpath)*/
    //preprocessing
    val sensorCleaner = cleanerSensorServiceHelper(CleanerService("preprocessingsensor")).clean(sensorDf)
    val siteCleaner = cleanerServiceHelper(CleanerService("preprocessingsite")).clean(siteDf)
    val meterCleaner = cleanerServiceHelper(CleanerService("preprocessingmeter")).clean(meterDf)
    //transformation
    val siteEnricher = EnricherSiteServiceHelper(EnricherService("enrichersite")).enrich(siteCleaner)
    val meterEnricher = EnricherMeterServiceHelper(EnricherService("enrichermeter")).enrich(meterCleaner)
    val allEnricher = EnricherAllServiceHelper(EnricherService("enricherall")).enrich(siteEnricher, meterEnricher, sensorCleaner, rawDataDf)
    //metrics
    val byFlluidByMeterPositionBySite = ComputeCalculationService().byFlluidByMeterPositionBySite(allEnricher, Columns.gb1.toString, Columns.gb2.toString, Columns.gb3.toString, Columns.gb4.toString, Columns.consumption.toString, Columns.aliasByFluidByMeterPositionBySite.toString)
    val byFlluidByMeterPositionForNSite = ComputeCalculationService().byFlluidByMeterPositionForNSite(allEnricher, Columns.gb1.toString, Columns.gb2.toString, Columns.gb3.toString, Columns.consumption.toString, Columns.aliasByFluidByMeterPositionForNSite.toString)
    val byTagBySite = ComputeCalculationService().byTagBySite(allEnricher, Columns.gb1.toString, Columns.gb4.toString, Columns.gb5.toString, Columns.consumption.toString, Columns.aliasByTagBySite.toString, Columns.column.toString, Columns.valu.toString)
    val byTagForNSite = ComputeCalculationService().byTagForNSite(allEnricher, Columns.gb1.toString, Columns.gb5.toString, Columns.consumption.toString, Columns.aliasByTagByNSite.toString, Columns.column.toString, Columns.valu.toString)


    //writer
    if (sensorpath.equals("src/main/resources/data/sensor.json")) {
      WriterService().writeCsv(byFlluidByMeterPositionBySite, settings.csvByFluidByMeterPositionBySite, settings.pathTargetByMeterPositionSite)
      WriterService().writeCsv(byFlluidByMeterPositionForNSite, settings.csvByFluidByMeterPositionNSite, settings.pathTargetByMeterPositionNSite)
      WriterService().writeCsv(byTagBySite, settings.csvByMeterPositionForNSite, settings.pathTargetByMeterPositionForNSite)
      WriterService().writeCsv(byTagForNSite, settings.csvByTagForNSite, settings.pathTargetByTagForNSite)
    } else {
      WriterService().writeCsv(byFlluidByMeterPositionBySite, settings.csvByFluidByMeterPositionBySite, settings.pathTargetTestByMeterPositionSite)
      WriterService().writeCsv(byFlluidByMeterPositionForNSite, settings.csvByFluidByMeterPositionNSite, settings.pathTargetTestByMeterPositionNSite)
      WriterService().writeCsv(byTagBySite, settings.csvByMeterPositionForNSite, settings.pathTestTargetByMeterPositionForNSite)
      WriterService().writeCsv(byTagForNSite, settings.csvByTagForNSite, settings.pathTestTargetByTagForNSite)

    }


    //Thread.sleep(999999999)
  }
}
