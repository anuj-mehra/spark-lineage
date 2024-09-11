package com.spike.lineage.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LocalRelation, Project, SerializeFromObject, Union}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConversions._
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.util.{Failure, Success, Try}

case class SequenceDependency(stepSequenceNumber: String, taskName: String, componentName: String, upstreamComponents: Seq[String])
case class LineageData(nanoTime: String, stepSequenceNumber: String, description: String, columnName: String,
                       derivationLogic: String, dependentOnColumn: Seq[String], dependentonSequenceNumbers: Seq[String])

object TryHandler {

  def handleTry[T](valueTry: Try[T]): Option[T] = {
    valueTry match {
      case Success(value) => Option(value)
      case Failure(exception) =>
        println(exception.getMessage)
        None
      case _ => None
    }
  }
}

object Implicits {
  implicit class MapExtension[TMK,TMV](val map:Map[TMK,TMV]){

    def getTypedValue[TV <: TMV](key: TMK): Option[TV] = {
      map.get(key).flatMap(v => TryHandler.handleTry(Try(v.asInstanceOf[TV])))
    }
  }
}

class SparkLineageGenerator(sparkSession: SparkSession) {

  val lineageData: java.util.concurrent.ConcurrentLinkedDeque[LineageData] =
    new java.util.concurrent.ConcurrentLinkedDeque[LineageData]()


  private def getNanotime: String = {
    val format = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss_SSSSSSSSS")
    val processingTime = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(System.currentTimeMillis()),
      ZoneId.systemDefault()).format(format)
    processingTime

  }

  // This method isused when we use df.cache or df.persist
 private def mapOneToOne(df:DataFrame, newDf: DataFrame, stepSequenceNo: String, description: String): Unit = {
   val nanoTime = System.nanoTime().toString

   val data = df.queryExecution.analyzed.output.sortBy(_.name).zip(
     newDf.queryExecution.analyzed.output.sortBy(_.name))
       .map(i => {
         val currentAttr = i._1
         val newAttr = i._2
         (nanoTime, stepSequenceNo, description,
         s"${newAttr.name}#${newAttr.exprId.id}", s"${currentAttr.name}#${currentAttr.exprId.id}",
         Seq(s"${currentAttr.name}#${currentAttr.exprId.id}"), Seq())
       })

   data.foreach(d => {
     val lineageInfo = LineageData(d._1, d._2, d._3, d._4, d._5, d._6, d._7)
     lineageData.add(lineageInfo)
   })
 }


  private def getColumns(childrenOfExpression: Seq[Expression]): Seq[String]={
    childrenOfExpression.flatMap(expression => expression.references.map(i => i.toString()))
  }

  def writePass1LineageJson: Unit = {
    import sparkSession.implicits._

    val df = lineageData.toSeq.toDF()
    df.show(false)
  }

  def writeLineageJson(sparkSession: SparkSession): Unit ={
    import sparkSession.sqlContext.implicits._
    val df = lineageData.toSeq.toDF
    df.orderBy("nanoTime").repartition(1).write.mode(SaveMode.Overwrite).json("hdfs-path")
  }

  // when we are manually capturing the lineage and not being done automatically
  def addLineageInfo(taskSequenceNumber: String, description: String, lineageInfo: Map[String, Seq[String]]): Seq[LineageData] = {
    val nanoTime = getNanotime

    val lineageDataTmp = lineageInfo.toSeq.map(x => {
      LineageData(
        nanoTime = nanoTime,
        stepSequenceNumber =  taskSequenceNumber,
        description = description,
        columnName = x._1,
        derivationLogic = "Manually added Lineage Info",
        dependentOnColumn = x._2,
        dependentonSequenceNumbers = Seq()
      )
    })

    lineageDataTmp.foreach(d=>{
      lineageData.add(d)
    })

    lineageDataTmp
  }


  def getLineage(df:DataFrame, stepSequenceNumber: String, description: String): Seq[LineageData] = {
    val nanoTime = this.getNanotime

    val data: Seq[LineageData] = df.queryExecution.analyzed match {
      case d:LogicalRDD => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case d:LocalRelation => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case d:LogicalRelation => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case d:Union =>
        val columns = d.output
        val d1 = d.output.zipWithIndex.map(i=> {
          d.children.map(_.output(i._2))
        })
        columns.zip(d1).map(k=> {
          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${k._1.name}#${k._1.exprId.id}",
            derivationLogic = "List()",
            dependentOnColumn = k._2.map(j => (s"${j.name}#${j.exprId.id}")),
            dependentonSequenceNumbers =  Seq())
        })
      case d:InMemoryRelation => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case d:Join => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case d:SerializeFromObject => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case project: Project => {
        val columns: Seq[NamedExpression] = project.projectList

        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case d:Filter => {
        val columns = d.output
        columns.map(i=> {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })
      }
      case aggregateInformation:Aggregate =>

        val groupColumns = aggregateInformation.groupingExpressions.map(_.asInstanceOf[AttributeReference])
        val aggregatedColumns = aggregateInformation.aggregateExpressions

        val d1 = groupColumns.map(i => {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })

        val d2 = aggregatedColumns.map(i => {
          val derivationLogic = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else i.children
          val dependentOnColumn = if(i.children.isEmpty) Seq(s"${i.name}#${i.exprId.id}") else getColumns(i.children)

          LineageData(nanoTime = nanoTime,
            stepSequenceNumber = stepSequenceNumber,
            description = description,
            columnName = s"${i.name}#${i.exprId.id}",
            derivationLogic = s"${derivationLogic}",
            dependentOnColumn = dependentOnColumn,
            dependentonSequenceNumbers =  Seq())
        })

        d2
      case _ => {
        Seq(LineageData(
          nanoTime = nanoTime,
          stepSequenceNumber = stepSequenceNumber,
          description = description,
          columnName = s"Yet to work on Something Unknown of Type: ${df.queryExecution.analyzed.getClass}",
          derivationLogic = "??",
          dependentOnColumn = Seq(),
          dependentonSequenceNumbers =  Seq()
        ))
      }

    }

    data.foreach(d=> {
      lineageData.add(d)
    })

    lineageData.toArray.toSeq.map(_.asInstanceOf[LineageData])
  }

}
