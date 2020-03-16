package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.signatures._
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

  def extractFeatures(r: ScoredDbRow, features: List[String]) = features.map {
    _ match {
      case x if ZHANG contains x         => scoreLens.get(r)
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
      case _                             => "Feature not found"
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
    val filterConcentrationSpecified = filters.getOrElse("concentration", List()) != List()
    def concentrationFilter(sample: DbRow): Boolean =
      if (filterConcentrationSpecified)
        filters("concentration").toSet
          .contains(sample.sampleAnnotations.sample.concentration.getOrElse("NA"))
      else // return all records
        true

    val filterProtocolSpecified = filters.getOrElse("protocol", List()) != List()
    def protocolFilter(sample: DbRow): Boolean =
      if (filterProtocolSpecified)
        filters("protocol").toSet
          .contains(sample.sampleAnnotations.sample.protocolname.getOrElse("NA"))
      else
        true

    val filterTypeSpecified = filters.getOrElse("type", List()) != List()
    def typeFilter(sample: DbRow): Boolean =
      if (filterTypeSpecified)
        filters("type").toSet.contains(sample.compoundAnnotations.compound.ctype.getOrElse("NA"))
      else
        true

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
        .filter(concentrationFilter)
        .filter(protocolFilter)
        .filter(typeFilter)
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
        List("zhang",
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
             "year")
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

  }

//   def topTable = result _

}
