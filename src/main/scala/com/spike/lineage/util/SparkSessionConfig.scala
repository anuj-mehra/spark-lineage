package com.spike.lineage.util

import org.apache.spark.sql.SparkSession

class SparkSessionConfig(jobName: String) {

  def get: SparkSession = {
    val sparkSessionBuilder: SparkSession.Builder = SparkSession
      .builder
      .appName(jobName)

    sparkSessionBuilder.master("local[*]")
    /*
        appConfig.environment match {
          case "dev" =>
            sparkSessionBuilder.master("local[*]")
          case "_" =>
        }*/

    sparkSessionBuilder.config("spark.driver.maxResultSize",0)
    sparkSessionBuilder.config("spark.sql.broadcastTimeout","36000")
    sparkSessionBuilder.config("spark.sql.autoBroadcastJoinThreshold",-1)

    sparkSessionBuilder.getOrCreate()
    /*val spark = sparkSessionBuilder.getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension","false")
    spark*/
  }
}

object SparkSessionConfig {

  def apply(jobName: String) = new SparkSessionConfig(jobName)
}
