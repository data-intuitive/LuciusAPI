package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object TargetsFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow],
                     genes: GenesDB,
                     version: String,
                     targetQuery: String,
                     limit: Int)

  type JobOutput = Array[Map[String, String]]

  val helpMsg =
    s"""Returns a list of targets based on a query, optionally with a limit on the number of results.
        |
      | Input:
        | - query: `startsWith` query on available known targets (mandatory)
        | - version: v1, v2 or t1 (optional, default is `v1`)
        | - limit: The result size is limited to this number (optional, default is 10)
     """.stripMargin

  def info(data: JobData) = s"Result for target query ${data.targetQuery}"

  def header(data: JobData) = "(target, #)"

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val JobData(db, genesDB, version, targetQuery, limit) = data
    implicit val genes = genesDB

    val resultRDD =
      db.rdd.map(_.compoundAnnotations)
        .distinct
        .filter(_.knownTargets != None)
        // Some entries in the ingested data set still contain nulls...
        .filter(!_.knownTargets.get.toSet.contains(null))
        .map(compoundAnnotations => compoundAnnotations.knownTargets.getOrElse(Seq()))
        .flatMap { kts =>
          kts
        }
        .filter { kt =>
          kt.startsWith(targetQuery)
        }
        .countByValue()
        .toArray
        .map { case (target, count) => Map("target" -> target, "count" -> count.toString) }

    val limitOutput = (resultRDD.size > limit)

    // Should we limit the result set?
    limitOutput match {
      case true  => resultRDD.take(limit)
      case false => resultRDD //.collect
    }

  }

//   def targets = result _

}
