package com.packtpub.mmlwspark.chapter3

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import water.support.{SparkContextSupport, SparkSessionSupport}


object  Ensemble_Methods_MultiClass extends SparkContextSupport with SparkSessionSupport {
  Logger.getRootLogger.setLevel(Level.WARN)

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Intro")
      // .master("spark://127.0.0.1:7077")
      .master("local[6]")
      .appName("Chapter3")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._ // import implicit conversions

    //@transient val h2oContext = H2OContext.getOrCreate(sc)
   // import h2oContext._
   // import h2oContext.implicits._

    val path = "/Users/olegbaydakov/IdeaProjects/detecting_dark_matter/src/main/resources/PAMAP2_Dataset/Protocol"
    val dataFiles = sc.wholeTextFiles(path)
    println(s"Number of input files: ${dataFiles.count}")

    // @Snippet
    val allColumnNames = Array(
        "timestamp", "activityId", "hr") ++ Array(
        "hand", "chest", "ankle").flatMap(sensor =>
        Array(
          "temp",
          "accel1X", "accel1Y", "accel1Z",
          "accel2X", "accel2Y", "accel2Z",
          "gyroX", "gyroY", "gyroZ",
          "magnetX", "magnetY", "magnetZ",
          "orient", "orientX", "orientY", "orientZ").
          map(name => s"${sensor}_${name}"))

   //println(allColumnNames.deep.mkString("\n"))

    // @Snippet
    val ignoredColumns =
      Array(0,
        3 + 13, 3 + 14, 3 + 15, 3 + 16,
        20 + 13, 20 + 14, 20 + 15, 20 + 16,
        37 + 13, 37 + 14, 37 + 15, 37 + 16)

    //println(ignoredColumns.deep.mkString("\n"))



    // @Snippet
    val rawData = dataFiles.flatMap { case (path, content) =>
      content.split("\n")
    }.map { row =>
      row.split(" ").
        map(_.trim).
        map(v => if (v.toUpperCase == "NAN") Double.NaN else v.toDouble).
        zipWithIndex.
        collect {
          case (cell, idx) if !ignoredColumns.contains(idx) => cell
        }
    }

    rawData.cache()

    // @Snippet
    import darkmatter.Tabulizer.table
    val columnNames = allColumnNames.
      zipWithIndex.
      filter { case (_, idx) => !ignoredColumns.contains(idx) }.
      map { case (name, _) => name }




    // @Snippet
    val activitiesMap = Map(
      1 -> "lying", 2 -> "sitting", 3 -> "standing", 4 -> "walking", 5 -> "running",
      6 -> "cycling", 7 -> "Nordic walking", 9 -> "watching TV", 10 -> "computer work",
      11 -> "car driving", 12 -> "ascending stairs", 13 -> "descending stairs", 16 -> "vacuum cleaning",
      17 -> "ironing", 18 -> "folding laundry", 19 -> "house cleaning", 20 -> "playing soccer",
      24 -> "rope jumping", 0 -> "other")


    // @Snippet
    val dataActivityId = rawData.map(l => l(0).toInt)

    val activityIdCounts = dataActivityId.
      map(n => (n, 1)).
      reduceByKey(_ + _)

    val activityCounts = activityIdCounts.
      collect.
      sortBy { case (activityId, count) =>
        -count
      }.map { case (activityId, count) =>
      (activitiesMap(activityId), count)
    }

    //println(s"Activities distribution:${table({activityCounts})}")


    val nanCountPerRow = rawData.map { row =>
        row.foldLeft(0) { case (acc, v) =>
          acc + (if (v.isNaN) 1 else 0)
        }
      }

    //nanCountPerRow.collect().take(5).foreach(println)

    val nanTotalCount = nanCountPerRow.sum
    val ncols = rawData.take(1)(0).length
    val nrows = rawData.count

    val nanRatio = 100.0 * nanTotalCount / (ncols * nrows)

    //println(f"""|NaN count = ${nanTotalCount}%.0f
    //            |NaN ratio = ${nanRatio}%.2f %%""".stripMargin)

    // @Snippet
    val nanRowDistribution = nanCountPerRow.
      map( count => (count, 1)).
      reduceByKey(_ + _).sortBy(-_._1).collect

    //println(s"${table(Seq("#NaN","#Rows"), nanRowDistribution, Map.empty[Int, String])}")


    // @Snippet
    val nanRowThreshold = 26
    val badRows = nanCountPerRow.zipWithIndex.zip(rawData).filter(_._1._1 > nanRowThreshold).sortBy(-_._1._1)
    //println(s"Bad rows (#NaN, Row Idx, Row):\n${badRows.collect.map(x => (x._1, x._2.mkString(","))).mkString("\n")}")


    // @Snippet
    val nanCountPerColumn = rawData.map { row => row.map(v => if (v.isNaN) 1 else 0)
    }.reduce((v1, v2) => v1.indices.map(i => v1(i) + v2(i)).toArray)



    //println(s"""Number of missing values per column:
    //           ^${table(columnNames.zip(nanCountPerColumn).map(t => (t._1, t._2, "%.2f%%".format(100.0 * t._2 / nrows))).sortBy(-_._2))}
    //           ^""".stripMargin('^'))

    // @Snippet
    val heartRateColumn = rawData.
      map(row => row(1)).
      filter(!_.isNaN).
      map(_.toInt)

    val heartRateValues = heartRateColumn.collect
    val meanHeartRate = heartRateValues.sum / heartRateValues.size
    scala.util.Sorting.quickSort(heartRateValues)
    val medianHeartRate = heartRateValues(heartRateValues.length / 2)

    println(s"Mean heart rate: ${meanHeartRate}")
    println(s"Median heart rate: ${medianHeartRate}")



    val nanColumnDistribV2 = rawData.map(row => {
      val activityId = row(0).toInt
      (activityId, row.drop(1).map(v => if (v.isNaN) 1 else 0))
    }).reduceByKey( (v1, v2) =>
      v1.indices.map(idx => v1(idx) + v2(idx)).toArray
    ).map { case (activityId, d) =>
      (activitiesMap(activityId), d)
    }.collect

   // println(s"""
   //            ^NaN Column x Response distribution V2:
   //            ^${table(Seq(columnNames.toSeq) ++ nanColumnDistribV2.map(v => Seq(v._1) ++ v._2), true)}
   //         """.stripMargin('^'))


    // @Snippet
    val imputedValues = columnNames.map {
      _ match {
        case "hr" => 60.0
        case _ => 0.0
      }
    }


    val rdd = sc.parallelize(Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9)))

    rdd.flatMap(row => (row.map(col => (col, row.indexOf(col))))) // flatMap by keeping the column position
      .map(v => (v._2, v._1)) // key by column position
      .groupByKey
      .sortByKey()   // regroup on column position, thus all elements from the first column will be in the first row
      .map(_._2).foreach(println)




    //h2oContext.stop(true)

  }
}
