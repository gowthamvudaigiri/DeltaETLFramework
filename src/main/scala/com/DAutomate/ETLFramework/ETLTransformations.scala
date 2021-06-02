package com.DAutomate.ETLFramework

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
object ETLTransformations {
  def transformAppend(spark:SparkSession, AppendDF : DataFrame , saveLocation: String ):Unit = {

    AppendDF
      .write
      .format("delta")
      .mode("append")
      .save(saveLocation)

  }

  def transformAppend(spark:SparkSession, AppendDF : DataFrame , saveLocation: String, PartitionBy: Array[String] ):Unit =  {

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

  def transformSCD1(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :String , ColMapping: Map[String , String] ):Unit ={

    TargetTable.as("Target")
      .merge(SourceDF.as("Source"), JoinKeys)
      .whenMatched("Source.Checksum <> Target.Checksum")
      .updateExpr(ColMapping)
      .whenNotMatched()
      .insertExpr(ColMapping)
      .execute()

  }

  def transformSCD2(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :Seq[String]  ):Unit =
  {



    var JoinKeysWithChecksum :Seq[String] =JoinKeys.union(Seq("Checksum"))
    generateJoinCondition(JoinKeysWithChecksum)
    generateJoinCondition(JoinKeys)
    val DFWithUpdateAndInsert= SourceDF
      .join(TargetTable.toDF.where("CurrentIndicator = true"), JoinKeysWithChecksum , "left_anti")
      .select(DataFrameCRUD.generateColumnList(SourceDF.schema):_*)
      .union(

    TargetTable.toDF.where("CurrentIndicator = true")
      .join(SourceDF, JoinKeys, "left_semi")
      .join(SourceDF, Seq("Checksum"), "left_anti")
      .select(DataFrameCRUD.generateColumnList(SourceDF.schema):_*))
      .withColumn("EndDate", lit("9999-12-31 00:00:00").cast(TimestampType))


    val ColMapping = DataFrameCRUD. generateColumnSourceTargetMapping(DFWithUpdateAndInsert.schema, "SCD2")


    TargetTable.as("Target")
      .merge(DFWithUpdateAndInsert.as("Source"), generateJoinCondition(JoinKeysWithChecksum))
      .whenMatched("Target.CurrentIndicator = true")
      .updateExpr(
        Map(
          "CurrentIndicator" -> "False",
          "EndDate" -> "current_timestamp()"
        )
      )
      .whenNotMatched()
      .insertExpr(ColMapping)
      .execute()


  }

  def generateJoinCondition(JoinKeys :Seq[String] ): String ={
    var joinCondition :String =""
    JoinKeys.foreach(column => joinCondition += " Source."+ column +" = target." +column + " and")
    return joinCondition.substring(1,joinCondition.length-3)

  }

}
