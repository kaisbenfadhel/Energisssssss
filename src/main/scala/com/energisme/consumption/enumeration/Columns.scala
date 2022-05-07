package com.energisme.consumption.enumeration

object Columns extends Enumeration {

  type Columns = Value


  
  val consumption = Value("multiplication")
  val gb1 = Value("client_code")
  val gb2 = Value("position")
  val gb3 = Value("meter_fluid")
  val gb4 = Value("code_site")
  val gb5 = Value("tag")
  val aliasByFluidByMeterPositionBySite = Value("ByFluidByMeterPositionBySite")
  val aliasByFluidByMeterPositionForNSite = Value("ByFluidByMeterPositionForNSite")
  val aliasByTagBySite = Value("ByTagBySite")
  val aliasByTagByNSite= Value("ByTagByNSite")
  val column = Value("tag")
  val valu = Value("unknown")
  val cd_mtr = Value("code_meter")
  val cd_ssr = Value("code_sensor")
  val ssr_id = Value("sensor_id")
  val tstmp = Value("timestamp")
  val ssr_ins = Value("sensor_installation")
  val ssr_unins = Value("sensor_uninstallation")
  val ssr_pul_wei = Value("sensor_pulse_weight")
  val vall = Value("value")

}
