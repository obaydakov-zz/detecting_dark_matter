//https://github.com/PacktPublishing/Mastering-Machine-Learning-with-Spark-2.x/blob/master/Chapter06/src/main/scala/com/github/maxpumperla/ml_spark/streaming/MSNBCPatternMining.scala

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.fpm.{FPGrowth, PrefixSpan}
import org.apache.spark.rdd.RDD




object frequent_pattern_mining {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val spark: SparkSession = SparkSession.builder()
      // .master("spark://127.0.0.1:7077")
    //  .master("yarn-client")
    //  .config("spark.local.ip", "127.0.0.1")
    //  .config("spark.driver.host", "127.0.0.1")
      .master("local[6]")
      .appName("MSNBC.com data pattern mining")
      // .config("fs.defaultFS", "hdfs://localhost:8020/")
    //  .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
    //  .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
    //  .enableHiveSupport()
      .getOrCreate


    spark.conf.set("spark.sql.shuffle.partitions", 6)

    //spark.conf.getAll.foreach (x => println (x._1))


  //  spark.sparkContext.setLogLevel("ERROR")

 //   val rootLogger = Logger.getRootLogger()
 //   rootLogger.setLevel(Level.ERROR)

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    println(sc.getConf.toDebugString)

    sc.addFile("src/main/resources/msnbc990928.seq")


    //val transactions: RDD[Array[Int]] = sc.textFile("file:///Users/olegbaydakov/IdeaProjects/detecting_dark_matter/src/main/resources/msnbc990928.seq") map { line =>
    //  line.split(" ").map(_.toInt)
    //}

    val transactions: RDD[Array[Int]] = sc.textFile(SparkFiles.get("msnbc990928.seq")) map { line =>
      line.split(" ").map(_.toInt)
    }

    // NOTE: Caching data is recommended
    val uniqueTransactions: RDD[Array[Int]] = transactions.map(_.distinct).cache()

    val fpGrowth = new FPGrowth().setMinSupport(0.01)
    val model = fpGrowth.run(uniqueTransactions)
    val count = uniqueTransactions.count()

  //  model.freqItemsets.collect().foreach { itemset =>

  //    if (itemset.items.length >= 3)
  //      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq / count.toDouble )
  //  }


    val rules = model.generateAssociationRules(confidence = 0.4)
  //  rules.collect().foreach { rule =>
  //    println("[" + rule.antecedent.mkString(",") + "=>"
  //      + rule.consequent.mkString(",") + "]," + (100 * rule.confidence).round / 100.0)
  //  }

    val frontPageConseqRules = rules.filter(_.consequent.head == 1)
    frontPageConseqRules.count
    frontPageConseqRules.filter(_.antecedent.contains(2)).count
    rules.filter(_.antecedent.contains(7)).count


    val sequences: RDD[Array[Array[Int]]] = transactions.map(_.map(Array(_))).cache()

    val prefixSpan = new PrefixSpan().setMinSupport(0.005).setMaxPatternLength(15)
    val psModel = prefixSpan.run(sequences)

  //  psModel.freqSequences.map(fs => (fs.sequence.length, 1))
  //    .reduceByKey(_ + _)
  //    .sortByKey()
  //    .collect()
  //    .foreach(fs => println(s"${fs._1}: ${fs._2}"))


  //  psModel.freqSequences
  //    .map(fs => (fs.sequence.length, fs))
  //    .groupByKey()
  //    .map(group => group._2.reduce((f1, f2) => if (f1.freq > f2.freq) f1 else f2))
  //    .map(_.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]"))
    //  .collect.foreach(println)

 //   psModel.freqSequences
 //     .map(fs => (fs.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]"), 1))
 //     .reduceByKey(_ + _).collect.foreach(println)
    //  .reduce( (f1, f2) => if (f1._2 > f2._2) f1 else f2 )


    psModel.freqSequences.collect().foreach {
      freqSequence =>
        println(
          freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") + ", " + freqSequence.freq
        )
    }




  }




}
