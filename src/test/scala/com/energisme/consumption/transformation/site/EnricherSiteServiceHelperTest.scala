package com.energisme.consumption.transformation.site

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.{CleanerService, EnricherService}
import com.energisme.consumption.preprocessing.cleanerServiceHelper
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class EnricherSiteServiceHelperTest extends FunSpec with Matchers with SparkSessionUtils {


  it("should enrich site DataFrame with function enrich(fusion 3 dataframes after explode 3 columns entry_meter,exit_meter,middle_meter and add position column ) ") {
    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)


    val preprocessingSiteleaner = new cleanerServiceHelper(CleanerService("preprocessingsite"))
    val sitecleanerDataFrame = preprocessingSiteleaner.clean(dataFrameSite)
    val siteEnricher = EnricherSiteServiceHelper(EnricherService("enrichersite")).enrich(sitecleanerDataFrame)
    val siteEntryAsList = siteEnricher
      .filter(siteEnricher("code_meter")
        .equalTo("meter_1"))
      .select("Position")
      .collectAsList()
    val siteExitAsList = siteEnricher
      .filter(siteEnricher("code_meter")
        .equalTo("meter_3"))
      .select("Position")
      .collectAsList()
    val siteMiddleAsList = siteEnricher
      .filter(siteEnricher("code_meter")
        .equalTo("meter_11"))
      .select("Position")
      .collectAsList()


    siteEnricher.columns should contain only("client_code", "code_site", "code_meter", "Position")
    siteEnricher.collect should have length 14
    assert("entry_position" == (siteEntryAsList.get(0)(0)) && "exit_position" == (siteExitAsList.get(0)(0)) && "middle_position" == (siteMiddleAsList.get(0)(0)))


  }


}
