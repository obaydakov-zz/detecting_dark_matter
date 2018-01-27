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


// ADD ENV VARAIBLES
// ADD core-site.xml, hidfs-site.xml, mapred-site.xml, yarn-site.xml to resoutce
// ADD jars from SPARK to project modules - File-> Project Structure - >Module->Dependencies


object advanced_analytics_spark {


  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(System.getenv("HADOOP_CONF_DIR"))
    println(System.getenv("HIVE_CONF_DIR"))
    println(System.getenv("HIVE_HOME"))
    println(System.getenv("SPARK_HOME"))

    System.setProperty("SPARK_YARN_MODE", "yarn")


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
    //  .config("spark.dynamicAllocation.enabled",true)
    //  .config("spark.shuffle.service.enabled",true)  //sbin/stop-shuffle-service.sh
    //    "spark.dynamicAllocation.enabled"=TRUE,
   // #    "spark.shuffle.service.enabled"=TRUE,
      .enableHiveSupport()
      .getOrCreate

    //  .config("hive.metastore.uris", "thrift://chddemo:9083")
    //  .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
    //  .config("fs.defaultFS", "maprfs://chddemo:7222")

    //println("ERROR "+ Log.valueOf("ERROR"))

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("hdfs://localhost:8020/data/linkage")

    parsed.show(5)

    /*spark.sql("""
      SELECT is_match, COUNT(*) cnt
      FROM linkage
      GROUP BY is_match
      ORDER BY cnt DESC""").show()*/

    val summary = parsed.describe()

    summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()
    summary.show()
    summary.printSchema()

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


