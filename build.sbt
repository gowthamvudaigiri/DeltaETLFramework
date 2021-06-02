organization := "com.DAutomate.ETLFramework"
name := "SparkETLFramework"

version := "0.1"

scalaVersion := "2.12.8" 

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.mortbay.jetty" % "jetty-util" % "6.1.25"
libraryDependencies += "io.delta" %% "delta-core" % "1.0.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "2.7.3"
