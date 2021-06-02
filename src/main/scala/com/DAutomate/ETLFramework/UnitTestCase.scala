package com.DAutomate.ETLFramework
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, TimestampType}

import scala.collection.mutable.ListBuffer

object UnitTestCase {

  var movieDF: DataFrame = null

  var movieSCD1: DataFrame = null

  var movieSCD2: DataFrame =null

  var movieUpdate:DataFrame =null

  val ColMapping: Map[String, String] = Map(
    "MovieID" -> "Source.MovieID",
    "MovieTitle" -> "Source.MovieTitle",
    "IMDBURL" -> "Source.IMDBURL",
    "ReleaseDate" -> "Source.ReleaseDate",
    "CheckSum" -> "Source.Checksum"

  )

  def generateDF(spark: SparkSession):Unit ={

    movieDF= DataFrameCRUD.generateDataFrameFromCSV(
      spark,
      Map(
        "sep" -> "|",
        "header" -> "true",
        "inferSchema" -> "true"
      ),
      "wasbs://movies@gowthamdlstorage.blob.core.windows.net/Movies",
      "Select MovieID, MovieTitle as Title, to_date(ReleaseDate,'dd-MMM-yyyy') as ReleaseDate, IMDBURL, year(to_date(ReleaseDate,'dd-MMM-yyyy'))as Year from Movies",
      "Movies"
    )

    movieSCD1 =movieDF.withColumn("Checksum", hash(generateColumnList(movieDF.schema):_*))

    movieSCD2 =movieDF
      .withColumn("Checksum", hash(generateColumnList(movieDF.schema):_*))
      .withColumn("StartDate", current_timestamp())
      .withColumn("EndDate", lit("9999-12-31 00:00:00").cast(TimestampType))
      .withColumn("CurrentIndicator", lit("true").cast(BooleanType))


    movieDF.createOrReplaceTempView("Movies")

    import spark.implicits._

    movieUpdate =spark.sql("Select MovieID, Title, case when Year%2 =0 then add_months(ReleaseDate,120) else ReleaseDate end as ReleaseDate, IMDBURL, case when Year%2 =0 then Year+10 else Year end as Year  from Movies").toDF()


  }


  def ETLTransformation_transformAppend_Test(spark: SparkSession) :Unit ={
    generateDF(spark)
    /*ETLTransformations.transformAppend(spark, movieDF, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieList")
    ETLTransformations.transformAppend(spark, movieSCD1, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieListSCD1")*/
    ETLTransformations.transformAppend(spark, movieSCD2, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieListSCD2" )
    //movieDF.show
  }


  def ETLTransformation_transformOverwrite_Test(spark: SparkSession) :Unit ={
    generateDF(spark)
    /*ETLTransformations.transformOverwrite(spark, movieDF, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieList")
    ETLTransformations.transformOverwrite(spark, movieSCD1, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieListSCD1")
    ETLTransformations.transformOverwrite(spark, movieSCD2, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieListSCD2" )*/
    ETLTransformations.transformOverwrite(spark, movieUpdate, "wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieUpdate" )
  }

  def ETLTransformation_transformSCD1_Test(spark: SparkSession) :Unit ={

    ETLTransformations.transformSCD1(
      spark,
      DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD1Data"),
      DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MoviesSCD1Data").toDF,
      "Source.MovieID = Target.MovieID",
      ColMapping)
  }

  def ETLTransformation_transformSCD2_Test(spark: SparkSession)={
    ETLTransformations.transformSCD2(
      spark,
      DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieListSCD2"),
      DataFrameCRUD.generateDataFrameFromType(spark, DeltaTable.forPath("wasbs://movies@gowthamdlstorage.blob.core.windows.net/MovieUpdate").toDF, "SCD1"),
      Seq("MovieID")
      )

  }

  def generateColumnList(DFSchema : StructType) :Seq[Column] ={
    var colList:ListBuffer[String] = ListBuffer()
    DFSchema.foreach(col =>{colList+= col.name})
    colList.map(name => col(name))
  }


}
