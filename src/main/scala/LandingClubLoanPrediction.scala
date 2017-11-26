// mastering machine learning spark 2.x book
package landingclub_loanprediction

import darkmatter.Tabulizer
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object LandingClubLoanPrediction {
  Logger.getRootLogger.setLevel(Level.WARN)

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Intro")
      // .master("spark://127.0.0.1:7077")
      .master("local[6]")
      .appName("LoanPrediction")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(sc)

    // Load data
    val USE_SMALLDATA = false
    val DATASET_DIR = "/Users/olegbaydakov/IdeaProjects/detecting_dark_matter/src/main/resources/LoanData"
    val DATASETS = Array("LoanStats3a.csv")
    import java.net.URI

    import water.fvec.H2OFrame
    val loanDataHf = new H2OFrame(DATASETS.map(name => URI.create(s"${DATASET_DIR}/${name}")):_*)


    import darkmatter.Tabulizer.table

    // @Snippet
    val idColumns = Seq("id", "member_id")
    val constantColumns = loanDataHf.names().indices
      .filter(idx => loanDataHf.vec(idx).isConst || loanDataHf.vec(idx).isBad)
      .map(idx => loanDataHf.name(idx))
    //println(s"Constant and bad columns: ${table(constantColumns, 4, None)}")


    // @Snippet
    val stringColumns = loanDataHf.names().indices
      .filter(idx => loanDataHf.vec(idx).isString)
      .map(idx => loanDataHf.name(idx))
    //println(s"String columns:${table(stringColumns, 4, None)}")

    // @Snippet
    val loanProgressColumns = Seq("funded_amnt", "funded_amnt_inv", "grade", "initial_list_status",
      "issue_d", "last_credit_pull_d", "last_pymnt_amnt", "last_pymnt_d",
      "next_pymnt_d", "out_prncp", "out_prncp_inv", "pymnt_plan",
      "recoveries", "sub_grade", "total_pymnt", "total_pymnt_inv",
      "total_rec_int", "total_rec_late_fee", "total_rec_prncp")
    // @Snippet
    val columnsToRemove = (idColumns ++ constantColumns ++ stringColumns ++ loanProgressColumns)

    val categoricalColumns = loanDataHf.names().indices
      .filter(idx => loanDataHf.vec(idx).isCategorical)
      .map(idx => (loanDataHf.name(idx), loanDataHf.vec(idx).cardinality()))
      .sortBy(-_._2)

    println(s"Categorical columns:${table(categoricalColumns)}")




  }
}
