package com.DAutomate.ETLFramework

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

  def transformSCD1(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :String , ColMapping: Map[String , String] ):Unit ={

    TargetTable.as("Target")
      .merge(SourceDF.as("Source"), JoinKeys)
      .whenMatched("Source.Checksum <> Target.Checksum")
      .updateExpr(ColMapping)
      .whenNotMatched()
      .insertExpr(ColMapping)
      .execute()

  }

  def transformSCD2(spark:SparkSession , TargetTable:DeltaTable , SourceDF : DataFrame , JoinKeys :Seq[String] , ColMapping: Map[String , String] ):Unit =
  {

    var JoinKeysWithChecksum :Seq[String] =JoinKeys.union(Seq("Checksum"))
    generateJoinCondition(JoinKeysWithChecksum)
    generateJoinCondition(JoinKeys)
    //SourceDF.join(TargetTable.toDF, JoinKeysWithChecksum , "left_anti").show(100)
  }

  def generateJoinCondition(JoinKeys :Seq[String] ): String ={
    var joinCondition :String =""
    JoinKeys.foreach(column => {
      if (column != "Checksum")
        joinCondition += " Source."+ column +" = target." +column + " and"
      else
        joinCondition += " Source."+ column +" <> target." +column +" and"

    })
    println(joinCondition.substring(1,joinCondition.length-3))
    return joinCondition.substring(1,joinCondition.length-3)

  }

}
