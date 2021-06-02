package com.DAutomate.ETLFramework

import org.apache.spark.sql.functions.{col, hash, lit, current_timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, TimestampType}


import scala.collection.mutable.ListBuffer

object DataFrameCRUD {



  def generateDataFrameFromCSV (spark:SparkSession, ConfigOption: Map[String , String], Location: String , SQL : String, TempViewName: String, Type: String ="Regular"):DataFrame={

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark.read.format("csv")
      .options(ConfigOption)
      .load(Location)
      .createOrReplaceTempView(TempViewName)

    import spark.sql
    generateDataFrameFromType(spark, sql(SQL).toDF, Type)
  }


  def generateDataFrameFromType(spark: SparkSession , SourceDF : DataFrame, Type : String):DataFrame ={

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


  def generateColumnList(DFSchema : StructType) :Seq[Column] ={
    var colList:ListBuffer[String] = ListBuffer()
    DFSchema.foreach(col =>{colList+= col.name})
    colList.map(name => col(name))
  }

  def generateColumnSourceTargetMapping(DFSchema : StructType, Type:String ="SCD1") :Map[String, String] ={
    val EndDate ="\"9999-12-31 00:00:00\""
    var ColMapping: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
    DFSchema.foreach(col =>{
      ColMapping.put(col.name , "Source."+col.name)
    })

    if(Type == "SCD2"){
      ColMapping.put("CurrentIndicator",  "True")
      ColMapping.put("StartDate", "current_timestamp()")
    }
    ColMapping.toMap
  }

}
