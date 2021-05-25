package com.DAutomate
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
object ETLTransformations {
  def transformAppend(spark:SparkSession, AppendDF : DataFrame , saveLocation: String ):Unit = {

    AppendDF
      .write
      .format("delta")
      .mode("append")
      .save(saveLocation)

   DeltaTable.forPath(saveLocation).vacuum()
  }

  def transformOverwrite(spark:SparkSession, AppendDF : DataFrame , saveLocation: String ):Unit ={

    AppendDF
      .write
      .format("delta")
      .mode("overwrite")
      .save(saveLocation)

    DeltaTable.forPath(saveLocation).vacuum()
  }
}
