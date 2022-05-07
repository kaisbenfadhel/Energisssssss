package com.energisme.consumption.transformation.site

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.{CleanerService, EnricherService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class SiteEnricherTest extends FunSpec with Matchers with SparkSessionUtils {

  it("should map (explode) columns  in function mapColumns ") {
    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)
    val preprocessingSiteleaner = new cleanerServiceHelper(CleanerService("preprocessingsite"))
    val sitecleanerDataFrame = preprocessingSiteleaner.clean(dataFrameSite)
    EnricherService("enrichersite").mapColumns(sitecleanerDataFrame, "middle_meter").collect should have length 2
  }
  it("should fusion three DataFrame with function fusionDataFrame where we used createMappedDataFrame and addColumnPosition functions ") {
    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)

    val preprocessingSiteleaner = new cleanerServiceHelper(CleanerService("preprocessingsite"))
    val sitecleanerDataFrame = preprocessingSiteleaner.clean(dataFrameSite)
    val siteEnriched = EnricherService("enrichersite").fusionDataFrame(sitecleanerDataFrame)

    val siteEntryAsList = siteEnriched
      .filter(siteEnriched("code_meter")
        .equalTo("meter_1"))
      .select("Position")
      .collectAsList()
    val siteExitAsList = siteEnriched
      .filter(siteEnriched("code_meter")
        .equalTo("meter_3"))
      .select("Position")
      .collectAsList()
    val siteMiddleAsList = siteEnriched
      .filter(siteEnriched("code_meter")
        .equalTo("meter_11"))
      .select("Position")
      .collectAsList()


    siteEnriched.columns should contain only("client_code", "code_site", "code_meter", "Position")
    siteEnriched.collect should have length 14
    assert("entry_position" == (siteEntryAsList.get(0)(0)) && "exit_position" == (siteExitAsList.get(0)(0)) && "middle_position" == (siteMiddleAsList.get(0)(0)))


  }


}
