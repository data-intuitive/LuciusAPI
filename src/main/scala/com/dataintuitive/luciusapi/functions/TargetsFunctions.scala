package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

object TargetsFunctions extends Functions {

  type Input = (RDD[DbRow], Genes)
  type Parameters = (String, String, Int)
  type Output = Array[Map[String, String]]

  val helpMsg =
    s"""Returns a list of targets based on a query, optionally with a limit on the number of results.
        |
      | Input:
        | - query: `startsWith` query on available known targets (mandatory)
        | - version: v1, v2 or t1 (optional, default is `v1`)
        | - limit: The result size is limited to this number (optional, default is 10)
     """.stripMargin

  def info(data:Input, par:Parameters) = s"Result for target query ${par._2}"

  def header(data:Input, par:Parameters) = "(target, #)"

  def result(data:Input, par:Parameters) = {

    val (db, genes) = data
    val (version, targetQuery, limit) = par

    val resultRDD =
      db
        .map(_.compoundAnnotations)
        .distinct
        .map(compoundAnnotations => 
            compoundAnnotations.knownTargets.getOrElse(Seq())
        )
        .flatMap{kts => kts}
        .filter{kt => 
            kt.startsWith(targetQuery)
        }
        .countByValue()
        .toArray
        .map{case(target, count) => Map("target" -> target, "count" -> count.toString)}

    val limitOutput = (resultRDD.size > limit)

    // Should we limit the result set?
    limitOutput match {
      case true  => resultRDD.take(limit)
      case false => resultRDD//.collect
    }

  }

  def targets = result _

}