ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

name := "projet_spark_streaming_m1"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "3.4.1",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "10.2.0.jre8",
  "com.typesafe" % "config" % "1.4.3"
)

fork := true
