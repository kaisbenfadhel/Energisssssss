package com.energisme.consumption.setting

import com.typesafe.config.Config

import scala.collection.JavaConverters._


class Settings(config: Config) extends Serializable {
  val sensorpath = config.getString("sensorPath")
  val sitepath = config.getString("sitePath")
  val meterpath = config.getString("meterPath")
  val rawpath = config.getString("rawPath")

  val csvByFluidByMeterPositionBySite = config.getStringList("ByFluidByMeterPositionBySite").asScala.toList
  val pathTargetByMeterPositionSite = config.getString("targetPathByFluidMeterPositionSite")


  val csvByFluidByMeterPositionNSite = config.getStringList("ByFluidByMeterPositionForNSite").asScala.toList
  val pathTargetByMeterPositionNSite = config.getString("targetPathByFluidMeterPositionNSite")


  val csvByMeterPositionForNSite = config.getStringList("ByMeterPositionForNSite").asScala.toList
  val pathTargetByMeterPositionForNSite = config.getString("targetPathByMeterPositionForNSite")


  val csvByTagForNSite = config.getStringList("ByFluidByTagForNSite").asScala.toList
  val pathTargetByTagForNSite = config.getString("pathTargetByTagForNSite")










  val pathTargetTestByMeterPositionSite = config.getString("targetTestPathByFluidMeterPositionSite")

  val pathTargetTestByMeterPositionNSite = config.getString("targetTestPathByFluidMeterPositionNSite")



  val pathTestTargetByMeterPositionForNSite = config.getString("targetTestPathByMeterPositionForNSite")


  val pathTestTargetByTagForNSite = config.getString("pathTestTargetByTagForNSite")


}