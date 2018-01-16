package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.CompoundAnnotations
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object TargetToCompoundsFunctions extends SessionFunctions {

  import com.dataintuitive.luciusapi.Common.Variables._

  case class JobData(db: Dataset[DbRow],
                     genes: Genes,
                     version: String,
                     targets: List[String], 
                     limit: Int)

  type JobOutput = Array[Map[String, Any]]


  def featureByLens[T](lens:CompoundAnnotations => T)(r:CompoundAnnotations) = lens(r)

  val extractJnjs = featureByLens(_.compound.jnjs.getOrElse("No Jnjs")) _
  val extractJnjb = featureByLens(_.compound.jnjb.getOrElse("No Jnjb")) _
  val extractSmiles = featureByLens(_.compound.smiles.getOrElse("No Smiles")) _
  val extractInchikey = featureByLens(_.compound.inchikey.getOrElse("No Inchikey")) _
  val extractCompoundname = featureByLens(_.compound.name.getOrElse("No Compound Name")) _
  val extractType = featureByLens(_.compound.ctype.getOrElse("No Compound Type")) _

  val extractTargets = featureByLens(_.getKnownTargets.toList) _

  def extractFeatures(r:CompoundAnnotations, features:List[String]) = features.map{ _ match {
    case x if JNJS contains x => extractJnjs(r)
    case x if JNJB contains x => extractJnjb(r)
    case x if SMILES contains x => extractSmiles(r)
    case x if INCHIKEY contains x => extractInchikey(r)
    case x if COMPOUNDNAME contains x => extractCompoundname(r)
    case x if TYPE contains x => extractType(r)
    case x if TARGETS contains x => extractTargets(r)
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

      val JobData(db, genes, version, targetQuery, limit) = data

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