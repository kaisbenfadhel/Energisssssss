name := "Energisme"

version := "1.2.8"



scalaVersion := "2.12.7"

coverageEnabled := true

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"    % "2.4.5"   % "provided",
  "org.apache.spark"  %%  "spark-sql"     % "2.4.5",
  "org.apache.spark"  %%  "spark-mllib"   % "2.4.5",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.2",
  "com.typesafe" % "config" % "1.4.2",
 // "log4j" % "log4j" % "1.2.17",

)
import sbtsonar.SonarPlugin.autoImport.sonarProperties

sonarProperties := Map(

  "sonar.host.url" -> "http://localhost:9000",

  "sonar.projectName" -> "Energisme",

  "sonar.projectKey" -> "energisme",

  "sonar.sources" -> "src/main/scala",

  "sonar.tests" -> "src/test/scala",

  "sonar.junit.reportPaths" -> "target/test-reports",

  "sonar.sourceEncoding" -> "UTF-8",

  "sonar.language" -> "scala",

  "sonar.scala.scoverage.reportPath" -> "target/scala-2.12/scoverage-report/scoverage.xml",

  "sonar.scala.scapegoat.reportPath" -> "target/scala-2.12/scapegoat-report/scapegoat.xml"


)
