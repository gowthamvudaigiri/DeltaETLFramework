organization := "com.DAutomate"
name := "SparkETLFramework"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"
libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "2.7.3"