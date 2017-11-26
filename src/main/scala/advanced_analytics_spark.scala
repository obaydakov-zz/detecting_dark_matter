package advanced.analytcs.spark

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.h2o.RDD
import org.apache.spark.h2o.utils.LogUtil
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import water.api.RequestServer
import water.support.SparkContextSupport
import water.util.LogBridge
 import org.apache.spark.sql._


object advanced_analytics_spark {


  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(System.getenv("HIVE_CONF_DIR"))

    import org.apache.spark.h2o._
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Intro")
      .master("spark://127.0.0.1:7077")
    //  .master("local[6]")
      .appName("Chapter2")
     // .config("fs.defaultFS", "hdfs://localhost:8020/")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
     .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate

    //  .config("hive.metastore.uris", "thrift://chddemo:9083")
    //  .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
    //  .config("fs.defaultFS", "maprfs://chddemo:7222")

    //println("ERROR "+ Log.valueOf("ERROR"))

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

  //  val parsed = spark.read
  //    .option("header", "true")
  //    .option("nullValue", "?")
  //    .option("inferSchema", "true")
  //    .csv("hdfs://localhost:8020/data/linkage")

    //parsed.show(5)

    /*spark.sql("""
      SELECT is_match, COUNT(*) cnt
      FROM linkage
      GROUP BY is_match
      ORDER BY cnt DESC""").show()*/

   // val summary = parsed.describe()

    //summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()
    //summary.show()
    //summary.printSchema()

    import spark.implicits._

  //  val schema = summary.schema
  //  summary.flatMap(row => {
  //    val metric = row.getString(0)
  //    (1 until row.size).map(i => (metric, schema(i).name, row.getString(i).toDouble))
  //  })
  //    .toDF("metric", "field", "value").show()

    sqlContext.sql("show databases").show()


  }


  }


