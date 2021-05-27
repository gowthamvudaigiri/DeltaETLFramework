package com.DAutomate

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{BooleanType, TimestampType}

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

  def unitTesting(spark: SparkSession):Unit = {

    var colList:ListBuffer[String] = ListBuffer()

    DeltaTable
      .forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesData")
      .toDF.schema.foreach(col =>{colList+= col.name})

    val List = colList.map(name => col(name))

    println(List.getClass.getTypeName)

    /*val sorDF =DeltaTable
      .forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesData")
      .toDF
      .where("MovieID >= 21")
      .withColumn("EvenMovieID", when(col("MovieID") <=30 ,add_months(col("ReleaseDate"),12)).otherwise(col("ReleaseDate")))
      .drop("ReleaseDate")
      .withColumnRenamed("EvenMovieID","ReleaseDate")
      .withColumn("StartDate", current_timestamp())
      .withColumn("EndDate", lit("9999-12-31 00:00:00").cast(TimestampType))
      .withColumn("CurrentIndicator", lit("true").cast(BooleanType))
      .withColumn("Checksum", hash(List:_*))
      .write
      .format("delta")
      .save("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD2Data")*/

    val ColMapping: Map[String, String] = Map(
      "MovieID" -> "Source.MovieID",
      "MovieTitle" -> "Source.MovieTitle",
      "IMDBURL" -> "Source.IMDBURL",
      "ReleaseDate" -> "Source.ReleaseDate",
      "CheckSum" -> "Source.Checksum"

    )


    ETLTransformations.transformSCD2(
      spark,
      DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD2Data"),
      DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesData").toDF.withColumn("Checksum", hash(List:_*)),
      Seq("MovieID", "Checksum"),
      ColMapping
    )



    // Append and Transform Unit Test Cases
    /* val movieDF =spark.read.format("csv")
       .option("inferSchema","true")
       .option("sep","|")
       .option("header","true")
       .load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/Movies")
       .select("MovieId","MovieTitle","ReleaseDate","IMDBURL")
       .withColumn("ReleaseDateTemp", to_date(col("ReleaseDate"), "dd-MMM-yyyy"))
       .drop(col("ReleaseDate"))
       .withColumnRenamed("ReleaseDateTemp", "ReleaseDate")


     val UpdatedmovieDF =movieDF.withColumn("dateadd", add_months(col("ReleaseDate"),12))
       .drop(col("ReleaseDate")).withColumnRenamed("dateadd","ReleaseDate")

     var colList:ListBuffer[String] = ListBuffer()

     movieDF.schema.foreach(col =>{colList+= col.name})

     val List = colList.map(name => col(name))


     ETLTransformations
       .transformAppend(spark, movieDF, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesData")


     ETLTransformations
     .transformAppend(spark, movieDF.withColumn("Checksum", hash(List:_*)) , "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD1Data")

     ETLTransformations
       .transformAppend(spark, UpdatedmovieDF, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesUpdateData")*/

    // SCD1 Test Cases



    /*
    //spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesData").show(5)
    //spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesUpdateData").show(5)
    spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD1Data").show(5)


    val ColMapping: Map[String, String] = Map(
      "MovieID" -> "Source.MovieID",
      "MovieTitle" -> "Source.MovieTitle",
      "IMDBURL" -> "Source.IMDBURL",
      "ReleaseDate" -> "Source.ReleaseDate",
      "CheckSum" -> "Source.Checksum"

    )


    var SourceDF =DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesUpdateData").toDF
    var colList:ListBuffer[String] = ListBuffer()
    SourceDF.schema.foreach(col =>{colList+= col.name})
    val List = colList.map(name => col(name))


    ETLTransformations.transformSCD1(spark,
      DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD1Data"),
      SourceDF.withColumn("Checksum", hash(List:_*)),
      "Source.MovieID = Target.MovieID",
      ColMapping)


    //spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesData").show(5)
    //spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesUpdateData").show(5)
    spark.read.format("delta").load("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD1Data").orderBy(col("MovieID")).show(5)*/
  }
}
