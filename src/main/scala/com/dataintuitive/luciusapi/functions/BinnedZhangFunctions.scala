package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciusapi.binning.BinningFunctions._

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.SignatureModel.SymbolSignature
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object BinnedZhangFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow],
                     genes: Genes,
                     version: String,
                     signatureQuery: Array[String],
                     binsX: Int,
                     binsY: Int,
                     filters: Map[String, String])

  type JobOutput = Seq[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data: JobData) = s""

  def header(data: JobData) = s""

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    val db = data.db.rdd
    val genes = data.genes
    val rawSignature = data.signatureQuery
    val binsX = data.binsX
    val binsY = data.binsY
    val filters = data.filters

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = SymbolSignature(rawSignature)

    // Filters
    val filterConcentrationSpecified = filters.getOrElse("concentration", "") != ""
    def concentrationFilter(sample: DbRow): Boolean =
      if (filterConcentrationSpecified)
        sample.sampleAnnotations.sample.concentration
          .getOrElse("NA")
          .matches(filters("concentration"))
      else
        true

    val filterProtocolSpecified = filters.getOrElse("protocol", "") != ""
    def protocolFilter(sample: DbRow): Boolean =
      if (filterProtocolSpecified)
        sample.sampleAnnotations.sample.protocolname.getOrElse("NA").matches(filters("protocol"))
      else
        true

    val filterTypeSpecified = filters.getOrElse("type", "") != ""
    def typeFilter(sample: DbRow): Boolean =
      if (filterTypeSpecified)
        sample.compoundAnnotations.compound.ctype.getOrElse("NA").matches(filters("type"))
      else
        true

    val symbolDict = genes.symbol2ProbesetidDict
    val indexDict = genes.index2ProbesetidDict
    val iSignature = signature
      .translate2Probesetid(symbolDict)
      .translate2Index(indexDict)

    val vLength = db.first.sampleAnnotations.t.get.length
    val query = signature2OrderedRankVector(iSignature, vLength)

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
      db.filter(concentrationFilter)
        .filter(protocolFilter)
        .filter(typeFilter)
        .flatMap { updateZhang(_, query) }

    val zhangStripped = zhangAdded.map(_._1)

    if (binsY > 0)
      bin2D(zhangStripped, binsX, binsY)
    else
      bin1D(zhangStripped, binsX)

  }

//   def binnedZhang = result _

}
