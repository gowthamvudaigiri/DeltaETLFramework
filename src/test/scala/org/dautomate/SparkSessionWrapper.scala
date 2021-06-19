package org.dautomate
import org.apache.spark.sql.SparkSession
trait SparkSessionWrapper {
  val spark = SparkSession.builder()
    .appName("SparkTestDeltaETLTransformations")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions" , "5")
    .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
}
