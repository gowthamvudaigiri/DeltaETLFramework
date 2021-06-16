package org.DAutomate
import org.scalatest.funspec.AnyFunSpec
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{to_date,col}


import io.delta.tables.DeltaTable
class DeltaETLTransformationsSpec extends  AnyFunSpec  with SparkSessionWrapper with TestVariablesandDeltaTablesPathWrapper{
  val log = Logger.getLogger("org")
  log.setLevel(Level.ERROR)

  describe("Test Append Transformation"){

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleaseDate, Year with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDelta,
        Type="Default"
      )

      assert(DeltaTable.forPath(MoviesDelta).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesDelta).toDF.schema.map(col => col.name) == MoviesExpectedColList )
    }

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleaseDate, Year, Checksum with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDeltaSCD1,
        Type="SCD1"
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

    deleteAllGeneratedFilesInTestFolder
  }
}
