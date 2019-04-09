package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.SignatureModel.SymbolSignature
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import com.dataintuitive.luciuscore.DbFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object CorrelationFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow],
                     genes: Genes,
                     version: String,
                     signatureQuery1: Array[String],
                     signatureQuery2: Array[String],
                     bins: Int,
                     filters: Map[String, List[String]])

  type JobOutput = Any // Seq[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data: JobData) = s""

  def header(data: JobData) = s""

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    val db = data.db.rdd
    val genes = data.genes
    val rawSignature1 = data.signatureQuery1
    val rawSignature2 = data.signatureQuery2
    val bins = data.bins
    val filters = data.filters

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature1 = SymbolSignature(rawSignature1)
    val signature2 = SymbolSignature(rawSignature2)

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

    val symbolDict = genes.symbol2ProbesetidDict
    val indexDict = genes.index2ProbesetidDict
    val iSignature1 = signature1
      .translate2Probesetid(symbolDict)
      .translate2Index(indexDict)
    val iSignature2 = signature2
      .translate2Probesetid(symbolDict)
      .translate2Index(indexDict)

    val vLength = db.first.sampleAnnotations.t.get.length
    val query1 = signature2OrderedRankVector(iSignature1, vLength)
    val query2 = signature2OrderedRankVector(iSignature2, vLength)
    val queries = List(query1, query2)

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
    val zhangScored: RDD[(DbRow, Seq[Double])] =
      db.filter(concentrationFilter)
        .filter(protocolFilter)
        .filter(typeFilter)
        .flatMap { row => queryDbRow(row, queries:_*) }

    import com.dataintuitive.luciusapi.binning.BinningFunctionsBis._

    implicit val dimension = bins
    val p1:Point = (BigDecimal(-1.0), BigDecimal(-1.0))
    val p4:Point = (BigDecimal(1.0), BigDecimal(1.0))

    val tiling = Tiling(dimension, Square(p1, p4))

    val hashedTiles = zhangScored
      .map{case (record, scores) => (BigDecimal(scores(0)), BigDecimal(scores(1)))}
      .map(x => tiling.inTile(x))
      .map(_.hash)
      .map((_, 1))

    val reduced = hashedTiles.reduceByKey(_+_)

    val binsWithCounts = reduced
      .collect
      .map{case (tileHash, count) => (inverseHash(tileHash), count)}
      .map{case (bin, v) => Map("x_bin_start" -> bin.x_bin_start, 
                                "x_bin_end" -> bin.x_bin_end,
                                "y_bin_start" -> bin.y_bin_start, 
                                "y_bin_end" -> bin.y_bin_end, 
                                "count" -> v)}
      .toList

    binsWithCounts

  }

   // def correlation = result _

}
