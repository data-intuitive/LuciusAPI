package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

object CompoundToSamplesFunctions extends Functions {

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

  def featureByLens[T](lens:DbRow => T)(r:DbRow) = lens(r)

  val extractPwid  = featureByLens(_.pwid.getOrElse("No PWID")) _

  val extractJnjs = featureByLens(_.compoundAnnotations.compound.jnjs.getOrElse("No Jnjs")) _
  val extractJnjb = featureByLens(_.compoundAnnotations.compound.jnjb.getOrElse("No Jnjb")) _
  val extractSmiles = featureByLens(_.compoundAnnotations.compound.smiles.getOrElse("No Smiles")) _
  val extractInchikey = featureByLens(_.compoundAnnotations.compound.inchikey.getOrElse("No Inchikey")) _
  val extractCompoundname = featureByLens(_.compoundAnnotations.compound.name.getOrElse("No Compound Name")) _
  val extractType = featureByLens(_.compoundAnnotations.compound.ctype.getOrElse("No Compound Type")) _

  val extractBatch = featureByLens(_.sampleAnnotations.sample.batch.getOrElse("No Batch id")) _
  val extractPlateid = featureByLens(_.sampleAnnotations.sample.plateid.getOrElse("No Plate id")) _
  val extractWell = featureByLens(_.sampleAnnotations.sample.well.getOrElse("No Well id")) _
  val extractProtocolname = featureByLens(_.sampleAnnotations.sample.protocolname.getOrElse("No Protocol")) _
  val extractConcentration = featureByLens(_.sampleAnnotations.sample.concentration.getOrElse("No Concentration")) _
  val extractYear = featureByLens(_.sampleAnnotations.sample.year.getOrElse("No Year")) _

  val extractTargets = featureByLens(_.compoundAnnotations.getKnownTargets.toList) _
  val extractSignificantGenes = featureByLens(_.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0)) _

  def extractFeatures(r:DbRow, features:List[String]) = features.map{ _ match {
    case x if PWID contains x => extractPwid(r)
    case x if JNJS contains x => extractJnjs(r)
    case x if JNJB contains x => extractJnjb(r)
    case x if SMILES contains x => extractSmiles(r)
    case x if INCHIKEY contains x => extractInchikey(r)
    case x if COMPOUNDNAME contains x => extractCompoundname(r)
    case x if TYPE contains x => extractType(r)
    case x if BATCH contains x => extractBatch(r)
    case x if PLATEID contains x => extractPlateid(r)
    case x if WELL contains x => extractWell(r)
    case x if PROTOCOLNAME contains x => extractProtocolname(r)
    case x if CONCENTRATION contains x => extractConcentration(r)
    case x if YEAR contains x => extractYear(r)
    case x if TARGETS contains x => extractTargets(r)
    case x if SIGNIFICANTGENES contains x => extractSignificantGenes(r)
    case _ => "Feature not found"
  }}


  val helpMsg =
    s"""Returns a list of samples matching a compound query (list).
        |
      | Input:
        | - query: List of compounds jnj to match (exact string match)
        | - version: v1, v2 or t1 (optional, default is `v1`)
     """.stripMargin

  def info(data:Input, par:Parameters) = s"Result for compound query ${par._2}"

  def header(data:Input, par:Parameters) = "All relevant data"

  def result(data:Input, par:Parameters) = {

    val (db, genes) = data
    val (version, compoundQuery, limit) = par

    // I could distinguish on version as well, but this makes more sense
    // This way, the same function can be reused for v1 and v2
    def isMatch(s: String, query:List[String]):Boolean = {
      // Exact match on one of the entries in the query
      query.toSet.contains(s)
    }

    val features = List("id","jnjs", "jnjb", "smiles", "inchikey",
      "compoundname", "Type", "targets", "batch",
      "plateid", "well", "protocolname", "concentration", "year", "significantGenes")

    val result =
      db
        .filter{sample =>
          sample.compoundAnnotations.compound.jnjs.exists(isMatch(_, compoundQuery))
        }
      .collect
      .map(entry => extractFeatures(entry, features) )

    result.map(_.zip(features).map(_.swap).toMap)


  }

  def compoundToSamples = result _

}