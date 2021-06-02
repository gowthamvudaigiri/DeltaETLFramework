package com.DAutomate.ETLFramework

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


import scala.collection.mutable.ListBuffer
object SparkETLFramework {

  def configDeltaStore(spark:SparkSession, SASKey:String):Unit ={
    spark.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.gowthamdlstorage.blob.core.windows.net", SASKey)
  }

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger("org")
    log.setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SparkExam")
      .master("local[*]")
      .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.AzureLogStore")
      .getOrCreate()


    configDeltaStore(spark, args(0))

    //UnitTestCase.ETLTransformation_transformAppend_Test(spark)
    UnitTestCase.ETLTransformation_transformSCD2_Test(spark)


    spark.close()
  }






}
