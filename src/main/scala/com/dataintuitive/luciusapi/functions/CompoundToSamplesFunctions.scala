package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object CompoundToSamplesFunctions extends SessionFunctions {

  import com.dataintuitive.luciusapi.Common.Variables._

  case class JobData(db: Dataset[DbRow],
                     genesDB: GenesDB,
                     version: String,
                     compounds: List[String],
                     limit: Int)

  type JobOutput = Array[Map[String, Any]]

  import com.dataintuitive.luciuscore.lenses.DbRowLenses._

  def extractFeatures(r: DbRow, features: List[String]) = features.map {
    _ match {
      case x if PWID contains x          => safePwidLens.get(r)
      case x if JNJS contains x          => safeJnjsLens.get(r)
      case x if JNJB contains x          => safeJnjbLens.get(r)
      case x if SMILES contains x        => safeSmilesLens.get(r)
      case x if INCHIKEY contains x      => safeInchikeyLens.get(r)
      case x if COMPOUNDNAME contains x  => safeNameLens.get(r)
      case x if TYPE contains x          => safeCtypeLens.get(r)
      case x if BATCH contains x         => safeBatchLens.get(r)
      case x if PLATEID contains x       => safePlateidLens.get(r)
      case x if WELL contains x          => safeWellLens.get(r)
      case x if PROTOCOLNAME contains x  => safeProtocolnameLens.get(r)
      case x if CONCENTRATION contains x => safeConcentrationLens.get(r)
      case x if YEAR contains x          => safeYearLens.get(r)
      case x if TARGETS contains x       => safeKnownTargetsLens.get(r)
      case x if TIME contains x          => safeTimeLens.get(r)
      case x if SIGNIFICANTGENES contains x => r.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0)
      case _                             => "Feature not found"
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

    val JobData(db, genesDB, version, compoundQuery, limit) = data
    implicit val genes = genesDB

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
      "significantGenes",
      "time"
    )

    val result =
      db.filter { sample =>
          sample.compoundAnnotations.compound.id.exists(isMatch(_, compoundQuery))
        }
        .collect
        .map(entry => extractFeatures(entry, features))

    result.map(_.zip(features).map(_.swap).toMap)

  }

//   def compoundToSamples = result _

}
