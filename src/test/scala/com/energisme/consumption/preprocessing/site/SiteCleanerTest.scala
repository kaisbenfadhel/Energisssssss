package com.energisme.consumption.preprocessing.site

import com.energisme.consumption.SparkSessionUtils
import com.energisme.consumption.interfaces.CleanerService
import com.energisme.consumption.reading.FileReader
import org.scalatest.{FunSpec, Matchers}

class SiteCleanerTest extends FunSpec with Matchers with SparkSessionUtils{

  it("should rename columns of the DataFrame Meter ") {

    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)

    CleanerService("preprocessingsite").RenameColumns(dataFrameSite).columns should contain only("client_code", "code_site", "entry_meter", "exit_meter","middle_meter")

  }


  it("should Replace   Null with array[] with Two parameter in function replaceNull ") {

    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)
    val siteDfRenamed = CleanerService("preprocessingsite").RenameColumns(dataFrameSite)


    val siteDfRenamedNotNull = CleanerService("preprocessingsite").replaceNull(siteDfRenamed, "middle_meter")
    val siteAsList = siteDfRenamedNotNull
      .filter(siteDfRenamedNotNull("code_site")
        .equalTo("site_1"))
      .select("middle_meter")
      .collectAsList()

    val middlemeter = siteAsList.get(0)(0)

    assert(Array().deep == (middlemeter))

  }

  it("should Replace   Null with array[] with One parameter in function replaceAllNull ") {

    val filePathSite = "src/test/resources/data/site.json"
    val dataFrameSite = FileReader.readJsonDataFrame(filePathSite)
    val siteDfRenamed = CleanerService("preprocessingsite").RenameColumns(dataFrameSite)


    val siteDfRenamedNotNull = CleanerService("preprocessingsite").replaceAllNull(siteDfRenamed)

    val siteAsList = siteDfRenamedNotNull
      .filter(siteDfRenamedNotNull("code_site")
        .equalTo("site_2"))
      .select("exit_meter")
      .collectAsList()

    val middlemeter = siteAsList.get(0)(0)

    assert(Array().deep == (middlemeter))

  }


}
