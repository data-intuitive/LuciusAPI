package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciusapi.binning.BinningFunctions._

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.SignatureModel.SymbolSignature
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

object HistogramFunctions extends Functions {

  type Input = (RDD[DbRow], Genes)
  type Parameters = (String, List[String], List[String], Int, Map[String, String])
  type Output = Array[Map[String, BigDecimal]]

  val helpMsg =
    s"""
     """.stripMargin


  def info(data:Input, par:Parameters) = s"Histogram"

  def header(data:Input, par:Parameters) = s"Selected features: ${par._3.toString}"

  def result(data:Input, par:Parameters) = {

    val (db, genes) = data
    val (version, signatureQuery, featuresQuery, nrBins, filters) = par

    val signatureSpecified = !(signatureQuery.headOption.getOrElse(".*") == ".*")
    val featuresSpecified = (featuresQuery.size >= 2)

    // Filters
    val filterConcentrationSpecified = filters.getOrElse("concentration", "") != ""
    def concentrationFilter(sample:DbRow):Boolean =
      if (filterConcentrationSpecified)
        sample.sampleAnnotations.sample.concentration.getOrElse("NA").matches(filters("concentration"))
      else
        true

    val filterProtocolSpecified = filters.getOrElse("protocol", "") != ""
    def protocolFilter(sample:DbRow):Boolean =
      if (filterProtocolSpecified)
        sample.sampleAnnotations.sample.protocolname.getOrElse("NA").matches(filters("protocol"))
      else
        true

    val filterTypeSpecified = filters.getOrElse("type", "") != ""
    def typeFilter(sample:DbRow):Boolean =
      if (filterTypeSpecified)
        sample.compoundAnnotations.compound.ctype.getOrElse("NA").matches(filters("type"))
      else
        true


    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = SymbolSignature(signatureQuery.toArray)

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

    // Add Zhang score if signature is present, select features where necessary
    // filter asap
    val zhangAndFeaturesAddedStrippedSorted =
      db
        .filter(concentrationFilter)
        .filter(protocolFilter)
        .filter(typeFilter)
        .flatMap {
          updateZhang(_, query)
        }
        .map{case (zhang, record) =>
          (zhang,
            record.compoundAnnotations.knownTargets.getOrElse(Seq()).toList.filter(kt => featuresQuery.contains(kt))
            )

        }
        // .sortBy(-_._1)

    if (featuresSpecified)
      histogram2D(zhangAndFeaturesAddedStrippedSorted, featuresQuery, nrBins, -1.0, 1.0)
    else
      histogram1D(zhangAndFeaturesAddedStrippedSorted.map(_._1), nrBins, -1.0, 1.0)

  }

  def histogram = result _

}