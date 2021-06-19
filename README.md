# DeltaETLFramework

This Library can be used along with Spark and Delta libraries to automate the Delta Table creation using Append, Overwrite and Merge(SCD1 & SCD2) Operations.
This Libraray requires 
##### 1. Delta  0.6.0 or higher
##### 2. Spark  2.4.5 or higher


Below are the list of static methods available in the library to automate the delta table creation
* transformAppend
* transformOverwrite
* transformSCD1
* transformSCD2

## transformAppend
This method take 5 arguments out of which 2 are optional
1. spark:SparkSession        - Pass your spark session variable.
2. SourceDF:DataFrame        - The Dataframe that you are trying to convert into Delta Table.
3. saveLocation:String       - Location where your Delta Table should exist. This can point to cloud storage/ hadoop /local file system.
4. PartitionBy :Seq[String]  - This is optional. Use this if you want to Partition your Delta Table.
5. Type :String              -This is optional. Use this if you want to create Delta Table with Type1 or Type2 Attributes.


#### Example to Create Delta Table with type as Default Partition By Year
```scala
import org.dautomate.DeltaETLTransformations._
import io.delta.tables.DeltaTable

 val TestResourcesPath =  new File(System.getProperty("user.dir")).toString+"/src/test/Resources"
    val MoviesFilePath: String = TestResourcesPath + "/Movies.csv"
    val MoviesUpdate1FilePath: String = TestResourcesPath +"/MoviesUpdated1.csv"
    val MoviesUpdate2FilePath: String = TestResourcesPath +"/MoviesUpdated2.csv"
    val MoviesDelta: String = TestResourcesPath +"/DeltaTables/MoviesDelta"
    val MoviesOverwrite: String = TestResourcesPath +"/DeltaTables/MoviesOverwrite"
    val MoviesDeltaSCD1: String = TestResourcesPath +"/DeltaTables/MoviesDeltaSCD1"
    val MoviesDeltaSCD2: String = TestResourcesPath +"/DeltaTables/MoviesDeltaSCD2"
		
transformAppend(spark,
        spark.read.format("csv")
		.option("header","True")
		.option("inferSchema","true")
		.option("Sep",",")
		.load(MoviesFilePath),
        MoviesDeltaSCD1,
        Type="Default",
        PartitionBy = Seq("Year")
      )
```

#### Example to Create Delta Table with type as SCD1 without Any Partitions
```scala
transformAppend(spark,
        spark.read.format("csv")
		.option("header","True")
		.option("inferSchema","true")
		.option("Sep",",").load(MoviesFilePath),
        MoviesDelta,
        Type="SCD1"
      )
```
#### Example to Create Delta Table with type as SCD2 Partition by Year
```scala
transformAppend(spark,
        spark.read.format("csv")
		.option("header","True")
		.option("inferSchema","true")
		.option("Sep",",")
		.load(MoviesFilePath),
        MoviesDeltaSCD2,
        Type="SCD2",
        PartitionBy = Seq("Year")
      )
```

## transformOverwrite
This method take 5 arguments out of which 2 are optional
1. spark:SparkSession        - Pass your spark session variable.
2. SourceDF:DataFrame        - The Dataframe that you are trying to convert into Delta Table.
3. saveLocation:String       - Location where your Delta Table should exist. This can point to cloud storage/ hadoop /local file system.
4. PartitionBy :Seq[String]  - This is optional. Use this if you want to Partition your Delta Table.
5. Type :String              -This is optional. Use this if you want to create Delta Table with Type1 or Type2 Attributes.

#### Example to overwrite MoviesSCD1 Delta Table with type as SCD1 without Any Partitions
```scala
transformOverwrite(spark,
        spark.read.format("csv")
		.option("header","True")
		.option("inferSchema","true")
		.option("Sep",",").load(MoviesFilePath),
        MoviesDeltaSCD1,
        Type="SCD1"
      )
```

## transformSCD1
This method take 4 arguments
1. spark:SparkSession        - Pass your spark session variable.
2. TargetTable:DeltaTable    - The Deltatable where we want to perform SCD1 Merge Operation.
3. SourceDF:DataFrame        - The dataframe to be used to perform SCD1 Merge Operation on Target Table.
4. JoinKeys:Seq[String]      - Key Attributes to perform SCD1.

#### Example to perform SCD1 Merge Operation on MoviesSCD1 Delta Table using MoviesUpdated1 Dataframe
```scala
transformSCD1(spark,
        DeltaTable.forPath(MoviesDeltaSCD1),
        spark.read.format("csv")
		.options(Map("sep"->",", "header" -> "true", "inferSchema" -> "true"))
		.load(MoviesUpdate1FilePath),
        Seq("MovieID")
```

## transformSCD2
This method take 4 arguments
1. spark:SparkSession        - Pass your spark session variable.
2. TargetTable:DeltaTable    - The Deltatable where we want to perform SCD1 Merge Operation.
3. SourceDF:DataFrame        - The dataframe to be used to perform SCD1 Merge Operation on Target Table.
4. JoinKeys:Seq[String]      - Key Attributes to perform SCD1.

#### Example to perform SCD2 Merge Operation on MoviesSCD2 Delta Table using MoviesUpdated1 Dataframe
```scala
transformSCD2(spark,
        DeltaTable.forPath(MoviesDeltaSCD2),
        spark.read.format("csv")
		.options(Map("sep"->",", "header" -> "true", "inferSchema" -> "true"))
		.load(MoviesUpdate1FilePath),
        Seq("MovieID")
```

I am few steps away from publish this Project as Artifact to maven repo. Meanwhile you can download the jar from the below location and attach it to your class path.
[Link to download the deltaetltransformations.jar](https://gowthamdlstorage.blob.core.windows.net/deltaetltransformations/DeltaETLFramework.jar)