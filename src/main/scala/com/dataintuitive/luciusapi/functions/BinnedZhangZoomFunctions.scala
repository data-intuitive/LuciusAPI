package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciusapi.binning.BinningFunctions._
import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.signatures._
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

object BinnedZhangZoomFunctions extends Functions {

  type Input = (RDD[DbRow], GenesDB)
  type Parameters = (Array[String], Int, Int, Int, Int)
  type Output = Seq[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data:Input, par:Parameters) = s""

  def header(data:Input, par:Parameters) = s""

  def result(data:Input, par:Parameters) = {

    val (db, genesDB) = data
    implicit val genes = genesDB
    val (rawSignature, binsX, binsY, x, y) = par

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = new SymbolSignature(rawSignature)

    val vLength = db.first.sampleAnnotations.t.get.length
    val query = signature.toIndexSignature.toOrderedRankVector(vLength)

    // Calculate Zhang score for all entries that contain a rank vector
    // This should be used in a flatMap
    def updateZhang(x:DbRow, query:Array[Double]):Option[(Double, DbRow)] = {
      x.sampleAnnotations.r match {
        case Some(r) => Some((connectionScore(r, query), x))
        case _ => None
      }
    }

    val zhangAdded = db.flatMap{updateZhang(_, query)}

    val maxIdx = zhangAdded.count

    // Index before (i) and after (j) sorting:
    val annotatedSimilarity =
      zhangAdded
        .zipWithIndex
        .sortBy{case ((z,record), i) => -z}
        .zipWithIndex
        .filter{case (((z,record), i), j) =>
          inBin2D((j,z), (x,y), (binsX, binsY), maxIdx)
        }
        .map{case (((z,record), i), j) => (j, z, i, record.pwid.getOrElse(""))}

    val result = annotatedSimilarity.collect

    result.map{case (i, z, j, pwid) => Map("indexNew" -> i, "indexOld" -> j, "zhang" -> z, "pwid" -> pwid)}


//    result.map{case (i, z, j, pwid) => Map("indexNew" -> i, "indexOld" -> j, "zhang" -> z, "pwid" -> pwid)}

  }

  def binnedZhangZoom = result _

}
