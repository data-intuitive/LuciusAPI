package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.CompoundAnnotations
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

object TargetToCompoundsFunctions extends Functions {

  type Input = (RDD[DbRow], Genes)
  type Parameters = (String, List[String], Int)
  type Output = Array[Map[String, Any]]


  val PWID =  Set("id", "pwid")
  val JNJS = Set("jnjs", "Jnjs")
  val JNJB = Set("jnjb", "Jnjb")
  val SMILES = Set("Smiles", "smiles", "SMILES")
  val INCHIKEY = Set("inchikey", "Inchikey")
  val COMPOUNDNAME = Set("compoundname", "CompoundName", "Compoundname", "name", "Name")
  val TYPE = Set("Type", "type")
  val BATCH = Set("batch", "Batch")
  val PLATEID = Set("plateid", "PlateId")
  val WELL = Set("well", "Well")
  val PROTOCOLNAME = Set("protocolname", "cellline", "CellLine", "ProtocolName")
  val CONCENTRATION = Set("concentration", "Concentration")
  val YEAR = Set("year", "Year")
  val TARGETS = Set("targets", "knownTargets", "Targets")
  val SIGNIFICANTGENES = Set("significantGenes")

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

  def info(data:Input, par:Parameters) = s"Result for target query ${par._2}"

  def header(data:Input, par:Parameters) = "All relevant data"

  def result(data:Input, par:Parameters) = {

    val (db, genes) = data
    val (version, targetQuery, limit) = par

    // I could distinguish on version as well, but this makes more sense
    // This way, the same function can be reused for v1 and v2
    def isMatch(targets: Seq[String], query:List[String]):Boolean = {
      // Exact match on one of the entries in the query
      targets.toList.map(target => query.toSet.contains(target)).foldLeft(false)(_||_)
    }

    val features = List("jnjs", "jnjb", "smiles", "inchikey", "compoundname", "Type", "targets")

    val resultRDD =
      db
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

  def targetToCompounds = result _

}