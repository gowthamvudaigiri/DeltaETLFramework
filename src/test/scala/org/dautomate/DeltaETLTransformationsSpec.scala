package org.dautomate
import org.scalatest.funspec.AnyFunSpec
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{to_date,col,year}


import io.delta.tables.DeltaTable
class DeltaETLTransformationsSpec extends  AnyFunSpec  with SparkSessionWrapper with TestVariablesandDeltaTablesPathWrapper{
  val log = Logger.getLogger("org")
  log.setLevel(Level.ERROR)

  describe("Test Append Transformation") {

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleaseDate, Year with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header", "True").option("inferSchema", "true").option("Sep", ",").load(MoviesFilePath),
        MoviesDelta,
        Type = "Default",
        PartitionBy = Seq("Year")
      )

      assert(DeltaTable.forPath(MoviesDelta).toDF.count() == 1682)
      assert(DeltaTable.forPath(MoviesDelta).toDF.schema.map(col => col.name) == MoviesExpectedColList)
    }

   it("This should create a DeltaTable with Attributes MovieID, Title, ReleaseDate, Year, Checksum with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDeltaSCD1,
        Type="SCD1",
        PartitionBy = Seq("Year")
      )

      assert(DeltaTable.forPath(MoviesDeltaSCD1).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesDeltaSCD1).toDF.schema.map(col => col.name) == MoviesSCD1ExpectedColList )
    }

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleaseDate, Year, Checksum, StartDate, EndDate, CurrentIndicator with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDeltaSCD2,
        Type="SCD2"
      )


      assert(DeltaTable.forPath(MoviesDeltaSCD2).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesDeltaSCD2).toDF.schema.map(col => col.name) == MoviesSCD2ExpectedColList )
    }

    it("This should create a InvalidTypeException") {
      assertThrows[DeltaETLTransformations.InvalidTypeException] {
        DeltaETLTransformations.transformAppend(spark,
          spark.read.format("csv").option("header", "True").option("inferSchema", "true").option("Sep", ",").load(MoviesFilePath),
          MoviesDelta,
          Type="Invalid"
        )}
    }

  }

  describe("Test Overwrite Transformation") {

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleaseDate, Year with 1682 Records and then Overwrite the data with same 1682 Records ") {
      DeltaETLTransformations.transformOverwrite(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesOverwrite,
        Type="Default",
        PartitionBy = Seq("Year")
      )

      DeltaETLTransformations.transformOverwrite(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesOverwrite,
        Type="Default",
        PartitionBy = Seq("Year")
      )

      assert(DeltaTable.forPath(MoviesOverwrite).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesOverwrite).toDF.schema.map(col => col.name) == MoviesExpectedColList )
    }

    it("This should create a InvalidTypeException") {
      assertThrows[DeltaETLTransformations.InvalidTypeException] {
        DeltaETLTransformations.transformOverwrite(spark,
          spark.read.format("csv").option("header", "True").option("inferSchema", "true").option("Sep", ",").load(MoviesFilePath),
          MoviesOverwrite,
          Type="Invalid"
        )}
    }

  }

 describe("Test SCD1 Transformation") {

    it("This should Update MovieID attribute in SCD1 DeltaTable from 1 to 10 to Year as 2000 and insert two new records with MovieID 2000, 2001"){
      DeltaETLTransformations.transformSCD1(spark,
        DeltaTable.forPath(MoviesDeltaSCD1),
        spark.read.format("csv").options(Map("sep"->",", "header" -> "true", "inferSchema" -> "true")).load(MoviesUpdate1FilePath),
        Seq("MovieID")
      )

      assert(DeltaTable.forPath(MoviesDeltaSCD1).toDF.count() == 1684)
      assert(DeltaTable.forPath(MoviesDeltaSCD1).toDF.filter("Year >=2000").count ==12)
    }

    it("This should Update MovieID attribute in SCD1 DeltaTable from 11 to 15 to Year as 2000 "){
      DeltaETLTransformations.transformSCD1(spark,
        DeltaTable.forPath(MoviesDeltaSCD1),
        spark.read.format("csv").options(Map("sep"->",", "header" -> "true", "inferSchema" -> "true")).load(MoviesUpdate2FilePath),
        Seq("MovieID")
      )

      assert(DeltaTable.forPath(MoviesDeltaSCD1).toDF.count() == 1684)
      assert(DeltaTable.forPath(MoviesDeltaSCD1).toDF.filter("Year >=2000").count ==17)
    }
  }

  describe("Test SCD2 Transformation") {

    it("This should Update MovieID attribute in SCD2 DeltaTable from 1 to 10 to Year as 2000 and insert two new records with MovieID 2000, 2001"){
      DeltaETLTransformations.transformSCD2(spark,
        DeltaTable.forPath(MoviesDeltaSCD2),
        spark.read.format("csv").options(Map("sep"->",", "header" -> "true", "inferSchema" -> "true")).load(MoviesUpdate1FilePath),
        Seq("MovieID")
      )

      assert(DeltaTable.forPath(MoviesDeltaSCD2).toDF.count() == 1694)
      assert(DeltaTable.forPath(MoviesDeltaSCD2).toDF.filter("Year >=2000").count ==12)
    }

    it("This should Update MovieID attribute in SCD2 DeltaTable from 11 to 15 to Year as 2000 "){
      DeltaETLTransformations.transformSCD2(spark,
        DeltaTable.forPath(MoviesDeltaSCD2),
        spark.read.format("csv").options(Map("sep"->",", "header" -> "true", "inferSchema" -> "true")).load(MoviesUpdate2FilePath),
        Seq("MovieID")
      )


      assert(DeltaTable.forPath(MoviesDeltaSCD2).toDF.count() == 1699)
      assert(DeltaTable.forPath(MoviesDeltaSCD2).toDF.filter("Year >=2000").count ==17)
      assert(DeltaTable.forPath(MoviesDeltaSCD2).toDF.filter("MovieID>=1 and MovieID<=15").count==30)
    }
  }

  describe("Test CDC Transformatation") (

    it("This should delete the records that did not exist in MovieDelete File and do required Updates and Inserts ") {
      DeltaETLTransformations.transformCDC(spark,
        DeltaTable.forPath(MoviesOverwrite),
        spark.read.format("csv").options(Map("sep" -> ",", "header" -> "true", "inferSchema" -> "true")).load(MoviesDeleteFilePath),
        Seq("MovieID")
      )


      assert(DeltaTable.forPath(MoviesOverwrite).toDF.count == 1674)
      assert(DeltaTable.forPath(MoviesOverwrite).toDF.filter("MovieID = 1683") .count ==1)
      assert(DeltaTable.forPath(MoviesOverwrite).toDF.filter("MovieID = 10 and Year =1997") .count ==1)
    }

  )

}
