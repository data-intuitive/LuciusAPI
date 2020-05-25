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
      case x if COMPOUND_ID contains x       => safeIdLens.get(r)
      case x if COMPOUND_SMILES contains x   => safeSmilesLens.get(r)
      case x if COMPOUND_INCHIKEY contains x => safeInchikeyLens.get(r)
      case x if COMPOUND_NAME contains x     => safeNameLens.get(r)
      case x if COMPOUND_TYPE contains x     => safeCtypeLens.get(r)
      case x if COMPOUND_TARGETS contains x  => safeKnownTargetsLens.get(r)
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

    val features = List(
      "compound_id",
      "compound_smiles",
      "compound_inchikey",
      "compound_name",
      "compound_type",
      "compound_targets"
    )

    val resultRDD =
      db.rdd
        .map(_.compoundAnnotations)
        .filter{compoundAnnotations =>
          compoundAnnotations.knownTargets.map(isMatch(_, targetQuery)).getOrElse(false)
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

    // Array(Map( "test" -> targetQuery))

    result
      .map(entry => extractFeatures(entry, features) )
      .map(_.zip(features).map(_.swap).toMap)

  }

//   def targetToCompounds = result _

}
