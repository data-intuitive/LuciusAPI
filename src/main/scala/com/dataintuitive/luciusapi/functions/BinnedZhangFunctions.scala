package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciusapi.binning.BinningFunctions._

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.signatures._
import com.dataintuitive.luciuscore.{Filter, QFilter, FilterFunctions}
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object BinnedZhangFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow],
                     genesDB: GenesDB,
                     version: String,
                     signatureQuery: Array[String],
                     binsX: Int,
                     binsY: Int,
                     filters: Seq[(String, Seq[String])])

  type JobOutput = Seq[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data: JobData) = s""

  def header(data: JobData) = s""

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    val db = data.db.rdd
    val genesDB = data.genesDB
    implicit val genes = genesDB
    val rawSignature = data.signatureQuery
    val binsX = data.binsX
    val binsY = data.binsY
    val qfilters = data.filters.map{ case(key,values) => QFilter(key, values) }

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = new SymbolSignature(rawSignature)

    val vLength = db.first.sampleAnnotations.t.get.length
    val query = signature.toIndexSignature.toOrderedRankVector(vLength)

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
      db
        .filter(row => FilterFunctions.isMatch(qfilters, row.filters))
        .flatMap { updateZhang(_, query) }

    val zhangStripped = zhangAdded.map(_._1)

    if (binsY > 0)
      bin2D(zhangStripped, binsX, binsY)
    else
      bin1D(zhangStripped, binsX)

  }

//   def binnedZhang = result _

}
