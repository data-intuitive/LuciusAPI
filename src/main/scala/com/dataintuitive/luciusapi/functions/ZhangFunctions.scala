package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.SignatureModel.SymbolSignature
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Map

object ZhangFunctions extends Functions {

  type Input = (RDD[DbRow], Genes)
  type Parameters = (Array[String], Boolean)
  type Output = Array[Map[String, Any]]

  val helpMsg =
    s"""
     """.stripMargin

  def info(data:Input, par:Parameters) = {
    val sortResult = par._2
      if (sortResult) "Zhang scores, sorted wrt descending Zhang score"
      else "Zhang scores, sorted wrt order in the database"
  }

  def header(data:Input, par:Parameters) = Seq("index after sorting", "zhang", "original index", "pwid").toString

  def result(data:Input, par:Parameters) = {

    val (db, genes) = data
    val (rawSignature, sortResult) = par

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = SymbolSignature(rawSignature)

    val symbolDict = genes.symbol2ProbesetidDict
    val indexDict = genes.index2ProbesetidDict
    val iSignature = signature
      .translate2Probesetid(symbolDict)
      .translate2Index(indexDict)

    val vLength = db.first.sampleAnnotations.t.get.length
    val query = signature2OrderedRankVector(iSignature, vLength)

    // Calculate Zhang score for all entries that contain a rank vector
    // This should be used in a flatMap
    def updateZhang(x:DbRow, query:Array[Double]):Option[(Double, DbRow)] = {
      x.sampleAnnotations.r match {
        case Some(r) => Some((connectionScore(r, query), x))
        case _ => None
      }
    }

    val zhangAdded = db.flatMap{updateZhang(_, query)}

    // Index before (i) and after (j) sorting:
    val annotatedSimilarity =
    if (sortResult)
      zhangAdded
        .zipWithIndex
        .sortBy{case ((z,x), i) => -z}
        .zipWithIndex
        .map{case (((z,x), i), j) => (j, z, i, x.pwid.getOrElse(""))}
    else
      zhangAdded
        .zipWithIndex
        .map{case ((z,x), i) => (i, z, i, x.pwid.getOrElse(""))}

    val result = annotatedSimilarity.collect

    result.map{case (i, z, j, pwid) => Map("indexNew" -> i, "indexOld" -> j, "zhang" -> z, "pwid" -> pwid)}

  }

  def zhang = result _

}