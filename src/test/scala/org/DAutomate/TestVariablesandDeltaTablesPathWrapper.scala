package org.DAutomate

import java.io.File
import org.apache.commons.io.FileUtils


trait TestVariablesandDeltaTablesPathWrapper {
    val TestResourcesPath =  new File(System.getProperty("user.dir")).toString+"/src/test/Resources"
    val MoviesFilePath: String = TestResourcesPath + "/Movies.csv"
    val MoviesUpdateFilePath: String = TestResourcesPath +"/MoviesUpdated.csv"
    val MoviesDelta: String = TestResourcesPath +"/MoviesDelta"
    val MoviesDeltaSCD1: String = TestResourcesPath +"/MoviesDeltaSCD1"
    val MoviesDeltaSCD2: String = TestResourcesPath +"/MoviesDeltaSCD2"
    val MoviesExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year")
    val MoviesSCD1ExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year", "Checksum")
    val MoviesSCD2ExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year", "Checksum", "StartDate", "EndDate", "CurrentIndicator")


  protected def deleteAllGeneratedFilesInTestFolder :Unit ={
    try {
      FileUtils.deleteDirectory(new File(MoviesDelta))
      FileUtils.deleteDirectory(new File(MoviesDeltaSCD1))
      FileUtils.deleteDirectory(new File(MoviesDeltaSCD2))
    }
    catch {
      case e: Exception => print(e.toString)

    }
  }
}
