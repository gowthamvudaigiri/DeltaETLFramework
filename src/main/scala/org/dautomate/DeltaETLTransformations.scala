package org.dautomate

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StructType, TimestampType}

import scala.collection.mutable.ListBuffer
object DeltaETLTransformations {


  def transformAppend(spark:SparkSession, SourceDF : DataFrame , saveLocation: String, PartitionBy: Seq[String] =Seq(),  Type : String ="Default" ):Unit = {
    CreateDeltaTable(spark, SourceDF, saveLocation, "append", PartitionBy, Type)
  }

  def transformOverwrite(spark:SparkSession, SourceDF : DataFrame , saveLocation: String, PartitionBy: Seq[String] =Seq(),Type : String ="Regular" ):Unit ={
    CreateDeltaTable(spark, SourceDF, saveLocation, "overwrite", PartitionBy, Type)
  }

  def transformSCD1(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :Seq[String] ):Unit ={

    val ColMapping = generateColumnSourceTargetMapping(generateDataFrameUsingType(spark, SourceDF, "SCD1").schema)

    TargetTable.as("Target")
      .merge(generateDataFrameUsingType(spark, SourceDF, "SCD1").as("Source"), generateJoinCondition(JoinKeys))
      .whenMatched("Source.Checksum <> Target.Checksum")
      .updateExpr(ColMapping)
      .whenNotMatched()
      .insertExpr(ColMapping)
      .execute()

  }

  def transformSCD2(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :Seq[String]  ):Unit = {

    val JoinKeysWithChecksum :Seq[String] =JoinKeys.union(Seq("Checksum"))
    val DFWithUpdateAndInsert= generateDataFrameUsingType(spark, SourceDF, "SCD1")
      .join(TargetTable.toDF.where("CurrentIndicator = true"), JoinKeysWithChecksum , "left_anti")
      .select(generateColumnList(generateDataFrameUsingType(spark, SourceDF, "SCD1").schema):_*)
      .union(

    TargetTable.toDF.where("CurrentIndicator = true")
      .join(generateDataFrameUsingType(spark, SourceDF, "SCD1"), JoinKeys, "left_semi")
      .join(generateDataFrameUsingType(spark, SourceDF, "SCD1"), Seq("Checksum"), "left_anti")
      .select(generateColumnList(generateDataFrameUsingType(spark, SourceDF, "SCD1").schema):_*))
      .withColumn("StartDate", current_timestamp())
      .withColumn("EndDate", lit("9999-12-31 00:00:00").cast(TimestampType))
      .withColumn("CurrentIndicator", lit("True").cast(BooleanType))



    val ColMapping = generateColumnSourceTargetMapping(DFWithUpdateAndInsert.schema)

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

  def transformCDC(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :Seq[String] ):Unit ={

    val ColMapping = generateColumnSourceTargetMapping(generateDataFrameUsingType(spark, SourceDF, "Default").schema)

    val TargetDataFrameWithDeleteIndicator = TargetTable.toDF.join(SourceDF , JoinKeys, "left_anti")
        .withColumn("DeleteIndicator", lit("True"))
        .union(SourceDF.withColumn("DeleteIndicator", lit("False"))
        )



    TargetTable.as("Target")
      .merge( TargetDataFrameWithDeleteIndicator.as("Source") , generateJoinCondition(JoinKeys))
      .whenMatched("Source.DeleteIndicator = True")
      .delete()
      .whenMatched()
      .updateExpr(ColMapping)
      .whenNotMatched()
      .insertExpr(ColMapping)
      .execute()


  }

  private def generateJoinCondition(JoinKeys :Seq[String] ): String ={
    var joinCondition :String =""
    JoinKeys.foreach(column => joinCondition += " Source."+ column +" = target." +column + " and")
   joinCondition.substring(1,joinCondition.length-3)

  }

  private def generateColumnSourceTargetMapping(DFSchema : StructType) :Map[String, String] ={
    var ColMapping: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    DFSchema.foreach(col => ColMapping.update(col.name , s"Source.${col.name}"))
    ColMapping.toMap
  }

  private def generateColumnList(DFSchema : StructType) :Seq[Column] ={
    var colList:ListBuffer[String] = ListBuffer()
    DFSchema.foreach(col =>{colList+= col.name})
    colList.map(name => col(name))
  }

  private def generateDataFrameUsingType(spark: SparkSession , SourceDF : DataFrame, Type : String):DataFrame ={

    if(Type == "SCD1")
      SourceDF.withColumn("Checksum", hash(generateColumnList(SourceDF.schema):_*))
    else if(Type == "SCD2")
      SourceDF
        .withColumn("Checksum", hash(generateColumnList(SourceDF.schema):_*))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit("9999-12-31 00:00:00").cast(TimestampType))
        .withColumn("CurrentIndicator", lit("true").cast(BooleanType))
    else
      SourceDF

  }

  private def CreateDeltaTable(spark:SparkSession, SourceDF : DataFrame , saveLocation: String, Mode:String, PartitionBy:Seq[String],  Type : String) :Unit ={

    if(Type!= "Default" && Type != "SCD1" && Type != "SCD2")
      throw new InvalidTypeException("Invalid Type Passed as Parameter. Allowed Type : Default | SCD1 | SCD2")

    if(PartitionBy.nonEmpty)
      generateDataFrameUsingType (spark, SourceDF, Type)
        .write
        .format ("delta")
        .mode (Mode)
        .partitionBy(PartitionBy :_ *)
        .save (saveLocation)
    else
      generateDataFrameUsingType (spark, SourceDF, Type)
        .write
        .format ("delta")
        .mode (Mode)
        .save (saveLocation)

   }

  implicit private[dautomate] class InvalidTypeException(Message: String) extends Exception(Message){}

}
