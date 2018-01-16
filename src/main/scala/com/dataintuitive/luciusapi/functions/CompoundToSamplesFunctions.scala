package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object CompoundToSamplesFunctions extends SessionFunctions {

  import com.dataintuitive.luciusapi.Common.Variables._

  case class JobData(db: Dataset[DbRow],
                     genes: Genes,
                     version: String,
                     compounds: List[String],
                     limit: Int)

  type JobOutput = Array[Map[String, Any]]

  def featureByLens[T](lens: DbRow => T)(r: DbRow) = lens(r)

  val extractPwid = featureByLens(_.pwid.getOrElse("No PWID")) _

  val extractJnjs = featureByLens(_.compoundAnnotations.compound.jnjs.getOrElse("No Jnjs")) _
  val extractJnjb = featureByLens(_.compoundAnnotations.compound.jnjb.getOrElse("No Jnjb")) _
  val extractSmiles = featureByLens(_.compoundAnnotations.compound.smiles.getOrElse("No Smiles")) _
  val extractInchikey = featureByLens(
    _.compoundAnnotations.compound.inchikey.getOrElse("No Inchikey")) _
  val extractCompoundname = featureByLens(
    _.compoundAnnotations.compound.name.getOrElse("No Compound Name")) _
  val extractType = featureByLens(
    _.compoundAnnotations.compound.ctype.getOrElse("No Compound Type")) _

  val extractBatch = featureByLens(_.sampleAnnotations.sample.batch.getOrElse("No Batch id")) _
  val extractPlateid = featureByLens(_.sampleAnnotations.sample.plateid.getOrElse("No Plate id")) _
  val extractWell = featureByLens(_.sampleAnnotations.sample.well.getOrElse("No Well id")) _
  val extractProtocolname = featureByLens(
    _.sampleAnnotations.sample.protocolname.getOrElse("No Protocol")) _
  val extractConcentration = featureByLens(
    _.sampleAnnotations.sample.concentration.getOrElse("No Concentration")) _
  val extractYear = featureByLens(_.sampleAnnotations.sample.year.getOrElse("No Year")) _

  val extractTargets = featureByLens(_.compoundAnnotations.getKnownTargets.toList) _
  val extractSignificantGenes = featureByLens(
    _.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0)) _

  def extractFeatures(r: DbRow, features: List[String]) = features.map {
    _ match {
      case x if PWID contains x             => extractPwid(r)
      case x if JNJS contains x             => extractJnjs(r)
      case x if JNJB contains x             => extractJnjb(r)
      case x if SMILES contains x           => extractSmiles(r)
      case x if INCHIKEY contains x         => extractInchikey(r)
      case x if COMPOUNDNAME contains x     => extractCompoundname(r)
      case x if TYPE contains x             => extractType(r)
      case x if BATCH contains x            => extractBatch(r)
      case x if PLATEID contains x          => extractPlateid(r)
      case x if WELL contains x             => extractWell(r)
      case x if PROTOCOLNAME contains x     => extractProtocolname(r)
      case x if CONCENTRATION contains x    => extractConcentration(r)
      case x if YEAR contains x             => extractYear(r)
      case x if TARGETS contains x          => extractTargets(r)
      case x if SIGNIFICANTGENES contains x => extractSignificantGenes(r)
      case _                                => "Feature not found"
    }
  }

  val helpMsg =
    s"""Returns a list of samples matching a compound query (list).
        |
      | Input:
        | - query: List of compounds jnj to match (exact string match)
        | - version: v1, v2 or t1 (optional, default is `v1`)
     """.stripMargin

  def info(data: JobData) = s"Result for compound query ${data.compounds}"

  def header(data: JobData) = "All relevant data"

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    val JobData(db, genes, version, compoundQuery, limit) = data

    // I could distinguish on version as well, but this makes more sense
    // This way, the same function can be reused for v1 and v2
    def isMatch(s: String, query: List[String]): Boolean = {
      // Exact match on one of the entries in the query
      query.toSet.contains(s)
    }

    val features = List(
      "id",
      "jnjs",
      "jnjb",
      "smiles",
      "inchikey",
      "compoundname",
      "Type",
      "targets",
      "batch",
      "plateid",
      "well",
      "protocolname",
      "concentration",
      "year",
      "significantGenes"
    )

    val result =
      db.filter { sample =>
          sample.compoundAnnotations.compound.jnjs.exists(isMatch(_, compoundQuery))
        }
        .collect
        .map(entry => extractFeatures(entry, features))

    result.map(_.zip(features).map(_.swap).toMap)

  }

//   def compoundToSamples = result _

}
