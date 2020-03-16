package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.Model.CompoundAnnotations
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object TargetToCompoundsFunctions extends SessionFunctions {

  import com.dataintuitive.luciusapi.Common.Variables._

  case class JobData(db: Dataset[DbRow],
                     genes: GenesDB,
                     version: String,
                     targets: List[String], 
                     limit: Int)

  type JobOutput = Array[Map[String, Any]]

  import com.dataintuitive.luciuscore.lenses.CompoundAnnotationsLenses._

  def extractFeatures(r:CompoundAnnotations, features:List[String]) = features.map{ 
    _ match {
      case x if JNJS contains x => safeJnjsLens.get(r)
      case x if JNJB contains x => safeJnjbLens.get(r)
      case x if SMILES contains x => safeSmilesLens(r)
      case x if INCHIKEY contains x => safeInchikeyLens.get(r)
      case x if COMPOUNDNAME contains x => safeNameLens.get(r)
      case x if TYPE contains x => safeCtypeLens.get(r)
      case x if TARGETS contains x => safeKnownTargetsLens.get(r)
      case _ => "Feature not found"
  }}


  val helpMsg =
    s"""Returns a list of compounds matching a target query (list).
        |
      | Input:
        | - query: List of targets to match (exact string match)
        | - version: v1, v2 or t1 (optional, default is `v1`)
     """.stripMargin

  def info(data:JobData) = s"Result for target query ${data.targets}"

  def header(data:JobData) = "All relevant data"

  def result(data:JobData)(implicit sparkSession: SparkSession) = {

    val JobData(db, genesDB, version, targetQuery, limit) = data
    implicit val genes = genesDB

    // I could distinguish on version as well, but this makes more sense
    // This way, the same function can be reused for v1 and v2
    def isMatch(targets: Seq[String], query:List[String]):Boolean = {
      // Exact match on one of the entries in the query
      targets.toList.map(target => query.toSet.contains(target)).foldLeft(false)(_||_)
    }

    val features = List("jnjs", "jnjb", "smiles", "inchikey", "compoundname", "Type", "targets")

    val resultRDD =
      db.rdd
        .map(_.compoundAnnotations)
        .filter{compoundAnnotations =>
          compoundAnnotations.knownTargets.map(isMatch(_, targetQuery)).getOrElse(false)  //.compound.jnjs.exists(isMatch(_, compoundQuery))
        }
      .distinct
      .sortBy(compoundAnnotations => compoundAnnotations.knownTargets.map(_.size).getOrElse(0))

    val limitOutput = (resultRDD.count > limit)

    // Should we limit the result set?
    val result = 
        limitOutput match {
            case true  => resultRDD.take(limit)
            case false => resultRDD.collect
        }

    result
        .map(entry => extractFeatures(entry, features) )
        .map(_.zip(features).map(_.swap).toMap)

  }

//   def targetToCompounds = result _

}
