
ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.11"
ThisBuild / scalaVersion := "2.12.15"
val sparkVersion = "3.3.4"
resolvers += "jitpack" at "https://jitpack.io"
val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion excludeAll (
    ExclusionRule("org.apache.hadoop")
    ) ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
  "org.apache.hadoop" % "hadoop-client" % "3.3.1" % "provided",
  "org.postgresql" % "postgresql" % "42.5.0",
  "com.amazonaws" % "aws-java-sdk-kms" % "1.12.66",
  "com.typesafe" % "config" % "1.4.2",
  "com.amazon.redshift" % "redshift-jdbc42" % "2.1.0.24",
  "com.softwaremill.macwire" %% "macros" % "2.4.0" % Provided,
  "org.apache.logging.log4j" % "log4j-core" % "2.22.1",
  "net.snowflake" % "snowflake-jdbc" % "3.16.0",
  "net.snowflake" %% "spark-snowflake" % "2.15.0-spark_3.3"









)
libraryDependencies ++=dependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "ecart-migration",


  )
