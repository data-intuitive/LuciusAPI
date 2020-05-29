package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciusapi.binning.BinningFunctions._

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.signatures._
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.{Filter, QFilter, FilterFunctions}
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import scala.collection.immutable.Map

object HistogramFunctions extends SessionFunctions {

  // import com.dataintuitive.luciusapi.Common.Filter

  case class JobData(db: Dataset[DbRow],
                     gene: GenesDB,
                     version: String,
                     signatureQuery: List[String],
                     featuresQuery: List[String],
                     bins: Int,
                     filters: Seq[(String, Seq[String])])

  type JobOutput = Array[Map[String, BigDecimal]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data: JobData) = s"Histogram"

  def header(data: JobData) = s"Selected features: ${data.featuresQuery}"

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val JobData(db, genesDB, version, signatureQuery, featuresQuery, nrBins, filters) = data
    implicit val genes = genesDB

    val qfilters = filters.map{ case(key,values) => QFilter(key, values) }

    val signatureSpecified = !(signatureQuery.headOption.getOrElse(".*") == ".*")
    val featuresSpecified = (featuresQuery.size >= 2)

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = new SymbolSignature(signatureQuery.toArray)
    val iSignature = signature.toIndexSignature

    val vLength = db.first.sampleAnnotations.t.get.length
    val query = iSignature.toOrderedRankVector(vLength)

    // Calculate Zhang score for all entries that contain a rank vector
    // This should be used in a flatMap
    def updateZhang(x: DbRow, query: Array[Double]): Option[(Double, DbRow)] = {
      x.sampleAnnotations.r match {
        case Some(r) => Some((connectionScore(r, query), x))
        case _       => None
      }
    }

    // Add Zhang score if signature is present, select features where necessary
    // filter asap
    val zhangAndFeaturesAddedStrippedSorted =
      db.rdd
        .filter(row => FilterFunctions.isMatch(qfilters, row.filters))
        .flatMap {
          updateZhang(_, query)
        }
        .map {
          case (zhang, record) =>
            (zhang,
             record.compoundAnnotations.knownTargets
               .getOrElse(Seq())
               .toList
               .filter(kt => featuresQuery.contains(kt)))

        }
    // .sortBy(-_._1)

    if (featuresSpecified)
      histogram2D(zhangAndFeaturesAddedStrippedSorted, featuresQuery, nrBins, -1.0, 1.0)
    else
      histogram1D(zhangAndFeaturesAddedStrippedSorted.map(_._1), nrBins, -1.0, 1.0)

  }

//   def histogram = result _

}
