ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "LogAnalysis_bigData"
  )

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.4"
libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies += "org.slf4j"%"slf4j-api"%"2.0.1"
libraryDependencies += "ch.qos.logback"%"logback-core"%"1.4.1"
libraryDependencies += "ch.qos.logback"%"logback-classic"%"1.4.1"
