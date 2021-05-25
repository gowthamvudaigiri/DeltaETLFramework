package com.DAutomate

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._
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

    unitTesting(spark)


    spark.close()
  }

  def unitTesting(spark: SparkSession):Unit ={
    ETLTransformations
      .transformAppend(spark, spark.range(10).toDF("Col"), "wasbs://movies@gowthamdlstorage.blob.core.windows.net/AppendData")

    ETLTransformations
      .transformOverwrite(spark, spark.range(10).toDF("Col"), "wasbs://movies@gowthamdlstorage.blob.core.windows.net/OverwriteData")

    spark.read.format("parquet").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/OverwriteData").orderBy(col("Col")).show(20)

    spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/AppendData").orderBy(col("Col")).show(20)


    val movieDF =spark.read.format("csv")
      .option("inferSchema","true")
      .option("sep","|")
      .option("header","true")
      .load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/Movies")
      .select("MovieId","MovieTitle","ReleaseDate","IMDBURL")
      .withColumn("ReleaseDateTemp", to_date(col("ReleaseDate"), "dd-MMM-yyyy"))
      .drop(col("ReleaseDate"))
      .withColumnRenamed("ReleaseDateTemp", "ReleaseDate")



    var colList:ListBuffer[String] = ListBuffer()

    movieDF.schema.foreach(col =>{colList+= col.name})

    val List = colList.map(name => col(name))

    movieDF.withColumn("Checksum", hash(List:_*)).show(100)
  }
}
