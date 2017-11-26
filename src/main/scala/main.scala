package com.packtpub.mmlwspark.chapter2

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.h2o.{H2OContext, H2OFrame, RDD}
import org.apache.spark.{SparkConf, SparkContext, h2o, mllib}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, _}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, _}
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.log4j.H2OPropertyConfigurator
import org.apache.log4j.LogManager
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.h2o.utils.LogUtil
import org.apache.spark.ml.Predictor
import water.api.RequestServer
import water.util.{Log, LogBridge}
import water.support.SparkContextSupport.addFiles
import water.support.H2OFrameSupport._
import ai.h2o.automl.AutoML
import ai.h2o.automl.AutoMLBuildSpec
import darkmatter.Tabulizer
import water.support.SparkContextSupport

object dark_matter extends SparkContextSupport{

  type Predictor = {
    def predict(features: Vector): Double
  }

  def computeMetrics(model: Predictor, data: RDD[LabeledPoint]): BinaryClassificationMetrics = {
    val predAndLabels = data.map(newData => (model.predict(newData.features), newData.label))
    new BinaryClassificationMetrics(predAndLabels)
  }

  def computeError(model: Predictor, data: RDD[LabeledPoint]): Double = {
    val labelAndPreds = data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    labelAndPreds.filter(r => r._1 != r._2).count.toDouble/data.count
  }




  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    //Logger.getLogger(classOf[RackResolver]).getLevel
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)


    // import org.apache.log4j.LogManager
    LogManager.getLogger(classOf[RequestServer]).setLevel(Level.OFF)
    LogManager.getLogger("water").setLevel(Level.OFF)
    LogManager.getLogger("h2o").setLevel(Level.OFF)

    // Setup log4j
    LogManager.getLogger("water.default").setLevel(Level.OFF)
    LogBridge.setH2OLogLevel(1)


    LogUtil.setH2OClientLogLevel("ERROR")
    //println(System.getenv("SPARK_LOCAL_IP"))

    import org.apache.spark.h2o._
    implicit val spark: SparkSession = SparkSession.builder()
     // .master("spark://127.0.0.1:7077")
        .master("yarn-client")
     .config("spark.local.ip", "127.0.0.1")
     .config("spark.driver.host", "127.0.0.1")
    //  .master("local[6]")
      .appName("Chapter2")
     // .config("fs.defaultFS", "hdfs://localhost:8020/")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
     .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate

    //println("ERROR "+ Log.valueOf("ERROR"))

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext




    //val ssc = new StreamingContext(sc, Durations.seconds(2))
    //ssc.start()
    //ssc.awaitTermination()


    /*val preview = spark.read.csv("src/main/resources/HIGGS.csv")
    //preview.show(5)
    //preview.printSchema()
    val summary = preview.describe()
    summary.show()*/

    val rawData1 = sc.textFile("src/main/resources/HIGGS.csv").take(1000)
    val rawData = sc.parallelize(rawData1)
    // @Snippet

    println(s"Number of rows: ${rawData.count}")

    // @Snippet
    //println("Rows")
    //println(rawData.take(2).mkString("\n"))

    // @Snippet
    val data = rawData.map(line => line.split(',').map(_.toDouble))

    // @Snippet
    val response: RDD[Int] = data.map(row => row(0).toInt)
    val features: RDD[Vector] = data.map(line => Vectors.dense(line.slice(1, line.size)))

    /*// @Snippet
    val featuresMatrix = new RowMatrix(features)
    val featuresSummary = featuresMatrix.computeColumnSummaryStatistics()

    // @Snippet
    import Tabulizer.table
    println(s"Higgs Features Mean Values = ${table(featuresSummary.mean, 8)}")
    println(s"Higgs Features Variance Values = ${table(featuresSummary.variance, 8)}")

    // @Snippet
    val nonZeros: Vector = featuresSummary.numNonzeros

    println(s"Non-zero values count per column: ${table(nonZeros, cols = 8, format = "%.0f")}")

    // @Snippet
    val numRows = featuresMatrix.numRows
    val numCols = featuresMatrix.numCols
    val colsWithZeros = nonZeros
      .toArray
      .zipWithIndex
      .filter { case (rows, idx) => rows != numRows }
    println(s"Columns with zeros:\n${table(Seq("#zeros", "column"), colsWithZeros, Map.empty[Int, String])}")

    // @Snippet
    val sparsity = nonZeros.toArray.sum / (numRows * numCols)
    println(f"Data sparsity: ${sparsity}%.2f")

    // @Snippet
    val responseValues = response.distinct.collect
    println(s"Response values: ${responseValues.mkString(", ")}")

    // @Snippet
    val responseDistribution = response.map(v => (v,1)).countByKey
    println(s"Response distribution:\n${table(responseDistribution)}") */


    //val h2oResponse = h2oContext.asH2OFrame(response, "response")



    //h2oContext.openFlow

    // @Snippet
    val higgs: RDD[LabeledPoint] = response.zip(features).map { case (response, features) => LabeledPoint(response, features) }

    higgs.setName("higgs").cache()


    //println("HIGGS "+higgs.collect().head)

    // @Snippet
    val trainTestSplits = higgs.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData: RDD[LabeledPoint]) = (trainTestSplits(0), trainTestSplits(1))


    MyDL(trainingData,testData)

   // MyGBM(trainingData,testData)

   // spark.stop()

  }

  def TreeModel(trainingData:RDD[LabeledPoint], testData: RDD[LabeledPoint]):Unit = {
    // @SkipCode
    println(
      """|
         | === Tree Model ===
         |""".stripMargin)

    // @Snippet
    val dtNumClasses = 2
    val dtCategoricalFeaturesInfo = Map[Int, Int]()
    val dtImpurity = "gini"
    val dtMaxDepth = 5
    val
    dtMaxBins = 10

    /// @Snippet
    val dtreeModel: DecisionTreeModel = DecisionTree.trainClassifier(trainingData,
      dtNumClasses,
      dtCategoricalFeaturesInfo,
      dtImpurity,
      dtMaxDepth,
      dtMaxBins)

    //println(s"Decision Tree Model:\n${dtreeModel.toDebugString}")

    // @Snippet
    val treeLabelAndPreds: RDD[(Int, Int)] = testData.map { point =>
      val prediction = dtreeModel.predict(point.features)
      (point.label.toInt,
        prediction.toInt)
    }

    val treeTestErr: Double = treeLabelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(f"Tree Model: Test Error = ${treeTestErr}%.3f")


    // println(treeLabelAndPreds.collect().take(10).foreach(println))

    // @Snippet
    val cm: Array[(Int, (Int, Int))] = treeLabelAndPreds.combineByKey(
      createCombiner = (label: Int) => if (label == 0) (1,0) else (0,1),
      mergeValue = (v: (Int,Int), label: Int) => if (label == 0) (v._1 +1, v._2) else (v._1, v._2 + 1),
      mergeCombiners = (v1: (Int,Int), v2: (Int,Int)) => (v1._1 + v2._1, v1._2 + v2._2)).collect

    //println("CM "+ cm.foreach(println))

    println(cm.deep.mkString("\n"))

    // @Snippet
    val (tn, tp, fn, fp) = (cm(0)._2._1, cm(1)._2._2, cm(1)._2._1, cm(0)._2._2)
    println(f"""Confusion Matrix
               |   ${0}%5d ${1}%5d  ${"Err"}%10s
               |0  ${tn}%5d ${fp}%5d ${tn+fp}%5d ${fp.toDouble/(tn+fp)}%5.4f
               |1  ${fn}%5d ${tp}%5d ${fn+tp}%5d ${fn.toDouble/(fn+tp)}%5.4f
               |   ${tn+fn}%5d ${fp+tp}%5d ${tn+fp+fn+tp}%5d ${(fp+fn).toDouble/(tn+fp+fn+tp)}%5.4f
               |""".stripMargin)


    val treeMetrics = computeMetrics(dtreeModel, testData)
    println(f"Tree Model: AUC on Test Data = ${treeMetrics.areaUnderROC()}%.3f")

  }

  def MyRandomForest(trainingData:RDD[LabeledPoint], testData: RDD[LabeledPoint]):Unit = {
      // @SkipCode RANDOM FOREST
      println("""|
                 | === Random Forest Model ===
                 |""".stripMargin)

      // @Snippet
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 10
      val featureSubsetStrategy = "auto"
      val impurity = "gini"
      val maxDepth = 5
      val maxBins = 10
      val seed = 42

    val rfModel = RandomForest.trainClassifier(trainingData,
      numClasses,
      categoricalFeaturesInfo,
      numTrees,
      featureSubsetStrategy,
      impurity,
      maxDepth,
      maxBins,
      seed)




      val rfTestErr = computeError(rfModel, testData)
      println(f"RF Model: Test Error = ${rfTestErr}%.3f")



      // @Snippet
      val rfMetrics = computeMetrics(rfModel, testData)
      println(f"RF Model: AUC on Test Data = ${rfMetrics.areaUnderROC}%.3f")

      // @Snippet
      val rfGrid =
        for (
          gridNumTrees <- Array(15, 20);
          gridImpurity <- Array("entropy", "gini");
          gridDepth <- Array(20, 30);
          gridBins <- Array(20, 50))
          yield {
            val gridModel = RandomForest.trainClassifier(trainingData, 2, Map[Int, Int](), gridNumTrees, "auto", gridImpurity, gridDepth, gridBins)
            val gridAUC = computeMetrics(gridModel, testData).areaUnderROC
            val gridErr = computeError(gridModel, testData)
            ((gridNumTrees, gridImpurity, gridDepth, gridBins), gridAUC, gridErr)
          }
      // @Snippet
      println(
        s"""RF Model: Grid results:
           ~${Tabulizer.table(Seq("trees, impurity, depth, bins", "AUC", "error"), rfGrid, format = Map(1 -> "%.3f", 2 -> "%.3f"))}
       """.stripMargin('~'))

      // @Snippet
      val rfParamsMaxAUC = rfGrid.maxBy(g => g._2)
      println(f"RF Model: Parameters ${rfParamsMaxAUC._1}%s producing max AUC = ${rfParamsMaxAUC._2}%.3f (error = ${rfParamsMaxAUC._3}%.3f)")


    }

  def MyGBM(trainingData:RDD[LabeledPoint], testData: RDD[LabeledPoint]):Unit = {
    // @SkipCode
    println("""|
               | === Gradient Boosted Trees Model ===
               |""".stripMargin)

    // @Snippet
    import org.apache.spark.mllib.tree.GradientBoostedTrees
    import org.apache.spark.mllib.tree.configuration.BoostingStrategy
    import org.apache.spark.mllib.tree.configuration.Algo

    val gbmStrategy = BoostingStrategy.defaultParams(Algo.Classification)
    gbmStrategy.setNumIterations(10)
    gbmStrategy.setLearningRate(0.1)
    gbmStrategy.treeStrategy.setNumClasses(2)
    gbmStrategy.treeStrategy.setMaxDepth(10)
    gbmStrategy.treeStrategy.setCategoricalFeaturesInfo(java.util.Collections.emptyMap[Integer, Integer])

    val gbmModel = GradientBoostedTrees.train(trainingData, gbmStrategy)

    // @Snippet
    val gbmTestErr = computeError(gbmModel, testData)
    println(f"GBM Model: Test Error = ${gbmTestErr}%.3f")

    val gbmMetrics = computeMetrics(gbmModel, testData)
    println(f"GBM Model: AUC on Test Data = ${gbmMetrics.areaUnderROC()}%.3f")

    // @Snippet
    val gbmGrid =
      for (
        gridNumIterations <- Array(5, 10, 50);
        gridDepth <- Array(2, 3, 5, 7);
        gridLearningRate <- Array(0.1, 0.01))
        yield {
          gbmStrategy.setNumIterations(gridNumIterations)
          gbmStrategy.treeStrategy.setMaxDepth(gridDepth)
          gbmStrategy.setLearningRate(gridLearningRate)

          val gridModel = GradientBoostedTrees.train(trainingData, gbmStrategy)
          val gridAUC = computeMetrics(gridModel, testData).areaUnderROC
          val gridErr = computeError(gridModel, testData)
          ((gridNumIterations, gridDepth, gridLearningRate), gridAUC, gridErr)
        }

    // @Snippet
    println(
      s"""GBM Model: Grid results:
         ~${Tabulizer.table(Seq("iterations, depth, learningRate", "AUC", "error"), gbmGrid.sortBy(-_._2).take(10), format = Map(1 -> "%.3f", 2 -> "%.3f"))}
       """.stripMargin('~'))

    // @Snippet
    val gbmParamsMaxAUC = gbmGrid.maxBy(g => g._2)
    println(f"GBM Model: Parameters ${gbmParamsMaxAUC._1}%s producing max AUC = ${gbmParamsMaxAUC._2}%.3f (error = ${gbmParamsMaxAUC._3}%.3f)")


  }


  def MyDL(trainingData:RDD[LabeledPoint], testData: RDD[LabeledPoint])(implicit spark:SparkSession):Unit = {
    // @SkipCode
    println("""|
               | === Deep Learning Model ===
               |""".stripMargin)


    val h2oContext: H2OContext = H2OContext.getOrCreate(spark)
    import h2oContext._
    import h2oContext.implicits._
    // @Snippet
    import spark.implicits._




    val trainingHF: H2OFrame = h2oContext.asH2OFrame(trainingData.toDF, "trainingHF")
    val testHF: H2OFrame = h2oContext.asH2OFrame(testData.toDF, "testHF")

    // @Snippet
    trainingHF.replace(0, trainingHF.vecs()(0).toCategoricalVec).remove()
    trainingHF.update()
    testHF.replace(0, testHF.vecs()(0).toCategoricalVec).remove()
    testHF.update()

    import _root_.hex.deeplearning._
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation

    val dlParams = new DeepLearningParameters()
    dlParams._train = trainingHF._key
    dlParams._valid = testHF._key
    dlParams._response_column = "label"
    dlParams._epochs = 1
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](500, 500, 500)

    // @Snippet
    val dlBuilder = new DeepLearning(dlParams)
    val dlModel = dlBuilder.trainModel.get

    // @Snippet
    println(s"DL Model: ${dlModel}")


    h2oContext.stop(true)



  }

}
