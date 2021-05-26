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

  }

  def transformOverwrite(spark:SparkSession, AppendDF : DataFrame , saveLocation: String ):Unit ={

    AppendDF
      .write
      .format("delta")
      .mode("overwrite")
      .save(saveLocation)

  }

  def transformSCD1(spark:SparkSession , TargetTable:DeltaTable , UpdateDF : DataFrame , JoinKeys :String , ColMapping: Map[String , String] ):Unit ={

    TargetTable.as("Target")
      .merge(UpdateDF.as("UpdateSor"), JoinKeys)
      .whenMatched("Source.Checksum <> Target.Checksum")
      .updateExpr(ColMapping)
      .whenNotMatched()
      .insertExpr(ColMapping)
      .execute()


  }
}
