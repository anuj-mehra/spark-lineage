package com.spike.lineage.app

import com.spike.lineage.util.{SparkLineageGenerator, SparkSessionConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object SparkJob extends App{

  implicit val sparkSession = SparkSessionConfig("SparkJob")
  val obj = new SparkJob
  obj.addRank(sparkSession.get)
}

class SparkJob extends Serializable {

  def addRank(implicit sparkSession: SparkSession): Unit = {

    val sparkLineageGenerator = new SparkLineageGenerator(sparkSession)
    val df = sparkSession.read.parquet("/Users/anujmehra/git/spark-lineage/src/main/resources/sampleData")

    sparkLineageGenerator.getLineage(df,"1","desc1")

    import org.apache.spark.sql.functions._
    val modifiedDf = df.select("name","age","salary")
      .groupBy("name").agg(sum("salary").as("total_salary"))
    sparkLineageGenerator.getLineage(modifiedDf,"2","desc3")
    //modifiedDf.show(false)


    val finalDf = modifiedDf.filter(col("name") === "anuj mehra")
    sparkLineageGenerator.getLineage(finalDf,"4","desc4")
    sparkLineageGenerator.writePass1LineageJson
  }
}
