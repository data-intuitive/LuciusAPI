package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.signatures._
import com.dataintuitive.luciuscore.Filter
import com.dataintuitive.luciuscore.Filters
import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object TopTableFunctions extends SessionFunctions {

  import com.dataintuitive.luciusapi.Common.Variables._

  case class JobData(db: Dataset[DbRow],
                     genes: GenesDB,
                     version: String,
                     head: Int,
                     tail: Int,
                     signatureQuery: List[String],
                     featuresQuery: List[String],
                     filters: Map[String, List[String]])

  type JobOutput = Array[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  import com.dataintuitive.luciuscore.lenses.ScoredDbRowLenses._

  import scalaz.Lens

  def extractFeatures(r: ScoredDbRow, features: List[String]) = features.map {
    _ match {
      // Calculated
      case x if ZHANG contains x         => scoreLens.get(r)
      // Sample
      case x if ID contains x            => safeIdLens.get(r)
      case x if BATCH contains x         => safeBatchLens.get(r)
      case x if PLATEID contains x       => safePlateidLens.get(r)
      case x if WELL contains x          => safeWellLens.get(r)
      case x if PROTOCOLNAME contains x  => safeProtocolnameLens.get(r)
      case x if CONCENTRATION contains x => safeConcentrationLens.get(r)
      case x if YEAR contains x          => safeYearLens.get(r)
      case x if TIME contains x          => safeTimeLens.get(r)
      // Compound
      case x if COMPOUND_ID contains x        => safeCompoundIdLens.get(r)
      case x if JNJB contains x               => safeJnjbLens.get(r)
      case x if COMPOUND_SMILES contains x    => safeSmilesLens.get(r)
      case x if COMPOUND_INCHIKEY contains x  => safeInchikeyLens.get(r)
      case x if COMPOUND_NAME contains x      => safeNameLens.get(r)
      case x if COMPOUND_TYPE contains x      => safeCtypeLens.get(r)
      case x if COMPOUND_TARGETS contains x   => safeKnownTargetsLens.get(r)
      // Filters
      case x if FILTERS contains x            => filtersLens.get(r)
      // fallback
      case _                                  => "Feature not found"
    }
  }

  def info(data: JobData) = s"Top Table wrt Zhang"

  def header(data: JobData) = s"Selected features: ${data.featuresQuery.toString}"

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    val JobData(db, genesDB, version, head, tail, signatureQuery, featuresQuery, filters) = data
    implicit val genes = genesDB

    val signatureSpecified = !(signatureQuery.headOption.getOrElse(".*") == ".*")
    val featuresSpecified = !(featuresQuery.headOption.getOrElse(".*") == ".*")

    // Filters
    // Map() means no filter is set
    val filtersSet = (filters.size > 0)

    // Turn the filter query into a Seq[Filter]
    val transformedFilters =
      filters
        .flatMap{ case(filterKey, values) => values.map(value => Filter("transformed_" + filterKey, value)) }
        .toSeq

    // The effective filtering, conditional
    def applyFilters(sample:DbRow):Boolean = {
      if (filtersSet) {
        sample.filters.isMatch(transformedFilters)
      }
      else
        true
    }

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = new SymbolSignature(signatureQuery.toArray)
    val iSignature = signature.toIndexSignature

    // Just taking the first element t vector is incorrect, what do we use options for after all?!
    // So... a version of takeWhile to the rescue
    val vLength = db.filter(_.sampleAnnotations.t.isDefined).first.sampleAnnotations.t.get.length
    val query = iSignature.toOrderedRankVector(vLength)

    // Calculate Zhang score for all entries that contain a rank vector
    // This should be used in a flatMap
    def updateZhang(x: DbRow, query: Array[Double]): Option[(Double, DbRow)] = {
      x.sampleAnnotations.r match {
        case Some(r) => Some((connectionScore(r, query), x))
        case _       => None
      }
    }

    // Add Zhang score if signature is present
    // Filter as soon as possible
    val zhangAdded: RDD[(Double, DbRow)] =
      db.rdd
        .filter(applyFilters _)
        .flatMap { updateZhang(_, query) }

    val topN =
      if (head > 0) {
        implicit val descendingOrdering = Ordering.by { x: (Double, DbRow) =>
          -x._1
        }
        zhangAdded
          .takeOrdered(head)
      } else {
        implicit val ascendingOrdering = Ordering.by { x: (Double, DbRow) =>
          x._1
        }
        zhangAdded
          .takeOrdered(tail)
      }

    // Create a feature selection, depending on the input
    // TODO: Add target information and make sure it gets parsed correctly!
    val features = {
      if (featuresSpecified) featuresQuery
      else
        List(
           "zhang",
           "id",
           "batch",
           "plateid",
           "well",
           "protocolname",
           "concentration",
           "year",
           "time",
           "compound_id",
           "compound_smiles",
           "compound_inchikey",
           "compound_name",
           "compound_type",
           "compound_targets",
           "filters"
       )
    }

    val result =
      if (head > 0)
        topN
          .sortBy { case (z, x) => -z }
          .map(entry => extractFeatures(entry, features))
      else
        topN
          .sortBy { case (z, x) => z }
          .map(entry => extractFeatures(entry, features))

    result.map(_.zip(features).map(_.swap).toMap)
    // Array(transformedFilters)

  }

//   def topTable = result _

}
