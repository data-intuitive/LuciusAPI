package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.signatures._
import com.dataintuitive.luciuscore.{Filter, QFilter, FilterFunctions}
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import com.dataintuitive.luciuscore.DbFunctions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object CorrelationFunctions extends SessionFunctions {

  // import com.dataintuitive.luciusapi.Common.Filter

  case class JobData(db: Dataset[DbRow],
                     genesDB: GenesDB,
                     version: String,
                     signatureQuery1: Array[String],
                     signatureQuery2: Array[String],
                     bins: Int,
                     filters: Seq[(String, Seq[String])])

  type JobOutput = Any // Seq[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data: JobData) = s""

  def header(data: JobData) = s""

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    val db = data.db.rdd
    implicit val genes = data.genesDB
    val rawSignature1 = data.signatureQuery1
    val rawSignature2 = data.signatureQuery2
    val bins = data.bins
    val qfilters = data.filters.map{ case(key,values) => QFilter(key, values) }

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature1 = new SymbolSignature(rawSignature1)
    val signature2 = new SymbolSignature(rawSignature2)

    val iSignature1 = signature1.toIndexSignature
    val iSignature2 = signature2.toIndexSignature

    val vLength = db.first.sampleAnnotations.t.get.length
    val query1 = iSignature1.toOrderedRankVector(vLength)
    val query2 = iSignature2.toOrderedRankVector(vLength)
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
      db
        .filter(row => FilterFunctions.isMatch(qfilters, row.filters))
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
