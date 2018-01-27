import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
//https://github.com/PacktPublishing/Mastering-Machine-Learning-with-Spark-2.x/blob/master/Chapter06/src/main/scala/com/github/maxpumperla/ml_spark/streaming/MSNBCStreamingExample.scala

object MSNBCStreamingExample {
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[6]")
      .appName("MSNBC data initial streaming example")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val ssc = new StreamingContext(sc, batchDuration = Seconds(10))

    val transactions: RDD[Array[Int]] = sc.textFile("file:///Users/olegbaydakov/IdeaProjects/detecting_dark_matter/src/main/resources/msnbc990928.seq") map { line =>
      line.split(" ").map(_.toInt)
    }

    val trainSequences: RDD[Array[Array[Int]]] = transactions.map(_.map(Array(_))).cache()
    val prefixSpan = new PrefixSpan().setMinSupport(0.005).setMaxPatternLength(15)
    val psModel = prefixSpan.run(trainSequences)
    val freqSequences = psModel.freqSequences.map(_.sequence).collect()


    val rawSequences: DStream[String] = ssc.socketTextStream("localhost", 9999)

    val sequences: DStream[Array[Array[Int]]] = rawSequences
      .map(line => line.split(" ").map(_.toInt))
      .map(_.map(Array(_)))

    print(">>> Analysing new batch of data")
    sequences.foreachRDD(
      rdd => rdd.foreach(
        array => {
          println(">>> Sequence: ")
          println(array.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]"))
          freqSequences.count(_.deep == array.deep) match {
            case count if count > 0 => println("is frequent!")
            case _ => println("is not frequent.")
          }
        }
      )
    )
    print(">>> done")

    ssc.start()
    ssc.awaitTermination()


  }

}
