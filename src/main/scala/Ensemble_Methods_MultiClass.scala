package com.packtpub.mmlwspark.chapter3

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import water.support.{SparkContextSupport, SparkSessionSupport}


object  Ensemble_Methods_MultiClass extends SparkContextSupport with SparkSessionSupport {
  Logger.getRootLogger.setLevel(Level.WARN)

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

  }
}
