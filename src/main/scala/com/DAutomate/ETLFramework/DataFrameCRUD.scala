package com.DAutomate.ETLFramework

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameCRUD {



  def generateDataFrameFromCSV (spark:SparkSession, ConfigOption: Map[String , String], Location: String , SQL : String, TempViewName: String):DataFrame={
    spark.read.format("csv")
      .options(ConfigOption)
      .load(Location)
      .createOrReplaceTempView(TempViewName)

    import spark.sql
    sql(SQL).toDF
  }

}
