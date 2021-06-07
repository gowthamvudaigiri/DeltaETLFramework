package org.DAutomate
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{to_date, col, year}
import io.delta.tables.DeltaTable
class DeltaETLTransformationsSpec extends  AnyFunSpec {
  val log = Logger.getLogger("org")
  log.setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SparkExam")
    .master("local[*]")
    .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.AzureLogStore")
    .config("spark.sql.shuffle.partitions" , "5")
    .getOrCreate()

  import spark.implicits._

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val MoviesFilePath: String =getClass.getResource("/TestResources/Movies.csv").getPath
  val MoviesUpdateFilePath: String = getClass.getResource("/TestResources/MoviesUpdated.csv").getPath
  val MoviesDelta :String = getClass.getResource("/TestResources/MoviesDelta/").getPath
  val MoviesDeltaSCD1 :String = getClass.getResource("/TestResources/MoviesDeltaSCD1/").getPath
  val MoviesDeltaSCD2 :String = getClass.getResource("/TestResources/MoviesDeltaSCD2/").getPath
  val MoviesExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year")
  val MoviesSCD1ExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year", "Checksum")
  val MoviesSCD2ExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year" ,"Checksum", "StartDate", "EndDate", "CurrentIndicator")

  MoviesExpectedColList.equals()


  describe("Test Append Transformation"){

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleseDate, Year with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDelta,
        "Default"
      )

      DeltaTable.forPath(MoviesDelta).toDF.count()
      assert(DeltaTable.forPath(MoviesDelta).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesDelta).toDF.schema.map(col => col.name) == MoviesExpectedColList )
    }

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleseDate, Year, Checksum with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDeltaSCD1,
        "SCD1"
      )

      DeltaTable.forPath(MoviesDelta).toDF.count()
      assert(DeltaTable.forPath(MoviesDelta).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesDelta).toDF.schema.map(col => col.name) == MoviesSCD1ExpectedColList )
    }

    it("This should create a DeltaTable with Attributes MovieID, Title, ReleseDate, Year, Checksum, StartDate, EndDate, CurrentIndicator with 1682 Records") {
      DeltaETLTransformations.transformAppend(spark,
        spark.read.format("csv").option("header","True").option("inferSchema","true").option("Sep",",").load(MoviesFilePath),
        MoviesDeltaSCD2,
        "SCD2"
      )

      DeltaTable.forPath(MoviesDelta).toDF.count()
      assert(DeltaTable.forPath(MoviesDelta).toDF.count() == 1682)
      assert( DeltaTable.forPath(MoviesDelta).toDF.schema.map(col => col.name) == MoviesSCD2ExpectedColList )
    }


    it("This should create a InvalidTypeException") {
      assertThrows[DeltaETLTransformations.InvalidTypeException] {
        DeltaETLTransformations.transformAppend(spark,
          spark.read.format("csv").option("header", "True").option("inferSchema", "true").option("Sep", ",").load(MoviesFilePath),
          MoviesDelta,
          "Invalid"
        )
      }}

    }
}
