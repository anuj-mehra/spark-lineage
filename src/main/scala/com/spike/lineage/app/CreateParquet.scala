package com.spike.lineage.app

import com.spike.lineage.util.SparkSessionConfig
import org.apache.spark.sql.SaveMode

object CreateParquet extends App{

  implicit val sparkSession = SparkSessionConfig("SparkJob").get
  val transactionsData = List(
    ("1", "anuj mehra", 40),
    ("2", "anuj mehra", 40),
    ("3", "priyanka goyal", 40),
    ("4", "priyanka goyal", 40),
    ("5", "priyanka goyal", 20),
    ("6", "priyanka goyal", 30),
    ("7", "mukesh ambani", 40),
    ("7", "anuj mehra", 40)
  )
  val schema = List("salary", "name", "age")

  import sparkSession.implicits._
  val df = transactionsData.toDF(schema:_*)

  df.write.mode(SaveMode.Overwrite).save("/Users/anujmehra/git/spark-lineage/src/main/resources/sampleData")

}
