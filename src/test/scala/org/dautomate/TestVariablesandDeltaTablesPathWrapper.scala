package org.dautomate

import java.io.File
import org.apache.commons.io.FileUtils


trait TestVariablesandDeltaTablesPathWrapper {
    val TestResourcesPath =  new File(System.getProperty("user.dir")).toString+"/src/test/Resources"
    val MoviesFilePath: String = TestResourcesPath + "/Movies.csv"
    val MoviesUpdate1FilePath: String = TestResourcesPath +"/MoviesUpdated1.csv"
    val MoviesUpdate2FilePath: String = TestResourcesPath +"/MoviesUpdated2.csv"
    val MoviesDeleteFilePath: String = TestResourcesPath +"/MoviesDelete.csv"
    val MoviesDelta: String = TestResourcesPath +"/DeltaTables/MoviesDelta"
    val MoviesOverwrite: String = TestResourcesPath +"/DeltaTables/MoviesOverwrite"
    val MoviesDeltaSCD1: String = TestResourcesPath +"/DeltaTables/MoviesDeltaSCD1"
    val MoviesDeltaSCD2: String = TestResourcesPath +"/DeltaTables/MoviesDeltaSCD2"
    val MoviesExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year")
    val MoviesSCD1ExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year", "Checksum")
    val MoviesSCD2ExpectedColList = List("MovieID", "Title", "ReleaseDate", "Year", "Checksum", "StartDate", "EndDate", "CurrentIndicator")

}
