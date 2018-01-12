package com.dataintuitive.luciusapi.deprecated

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import com.dataintuitive.luciuscore.TransformationFunctions
import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.SignatureModel._
import com.dataintuitive.luciuscore.utilities.SignedString

import scala.util.Try

object signature extends SparkJob with NamedRddSupport with Globals {

  import OldCommon._

  val simpleChecks:SingleParValidations = Seq()

  val combinedChecks:CombinedParValidations = Seq()

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    SparkJobValid

//    val showHelp = Try(config.getString("help")).toOption.isDefined
//    val testsSingle = runSingleParValidations(simpleChecks, config)
//    val testsCombined = runCombinedParValidations(combinedChecks, config)
//    val allTests = aggregateValidations(testsSingle ++ testsCombined)
//
//    (showHelp, allTests._1) match {
//      case (true, _) => SparkJobInvalid(help)
//      case (false, true) => SparkJobValid
//      case (false, false) => SparkJobInvalid(allTests._2)
//    }
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Compound query string
    val compound: String = Try(config.getString("compound")).getOrElse("*")

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint functions
    //    val input = (db, genes)
    //    val parameters = signatureQuery

    // Create dictionary based on l1000 genes
    val probesetid2symbol = genes.genes.map(x => (x.probesetid, x.symbol))
    val ensemblid2symbol = genes.genes.map(x => (x.ensemblid, x.symbol))
    val entrezid2symbol = genes.genes.map(x => (x.entrezid, x.symbol))
    val symbol2symbol = genes.genes.map(x => (x.symbol, x.symbol))

    val tt = probesetid2symbol ++ ensemblid2symbol ++ entrezid2symbol ++ symbol2symbol


    // Invert the translation table, but remove identities. This way, we show the gene symbols.
    // Be careful, the translation_table should not be a map yet, because that way some of
    // the entries are removed!
    val itt = tt.map(_.swap).filter { case (x, y) => x != y }.toMap

    // Start with query compound and retrieve indices
    val selection =
    db
      .filter(x => x.compoundAnnotations.compound.jnjs.exists(_.matches(compound)))
      .collect
      .map(x => (x.sampleAnnotations.t.get, x.sampleAnnotations.p.get))
    //    val tp = selection.
    //      flatMap{
    //        x => x.sampleAnnotations.p.get.zip(x.sampleAnnotations.p.get)
    //      }
    val valueVector = TransformationFunctions.aggregateStats(selection, 0.05)
    val rankVector = TransformationFunctions.stats2RankVector((valueVector, Array()))

    // Convert rank vector to index signature
    // This is the poor man's approach, not taking into account duplicate entries and such.
    // Be careful, signature and vector indices are 1-based
    def rankVector2IndexSignature(v: RankVector) = {

      // Be careful: offset 1 for vectors for consistency!
      def nonZeroElements(v: RankVector, offset: Int = 1): Array[(Index, Rank)] = {
        v.zipWithIndex
          .map(x => (x._1, x._2 + offset))
          .map(_.swap)
          .filter(_._2 != 0.0)
      }

      val nonzero = nonZeroElements(v)
      val asArrayInt = nonzero.map {
        case (unsignedIndex, signedRank) => ((signedRank.abs / signedRank) * unsignedIndex).toInt
      }
      val asArrayString = asArrayInt.map(_.toString)
      //      new IndexSignature(asArrayString)
      asArrayString
    }

    val indexSignature: SignatureType = rankVector2IndexSignature(rankVector)

    // dict's
    val symbolDict = genes.symbol2ProbesetidDict
    val inverseSymbolDict = symbolDict.map(_.swap)
    val indexDict = genes.index2ProbesetidDict

    // Transformation index => probesetid
    implicit def stringExtension(string: String): SignedString = new SignedString(string)

    val probesetidSignature =
      indexSignature.map { g =>
        val translation = indexDict.get(g.abs.toInt)
        translation.map(go => g.sign + go)
      }.map(_.getOrElse("OOPS"))

    // Transform to symbol
    val symbolSignature =
    probesetidSignature.map { g =>
      val translation = inverseSymbolDict.get(g.abs)
      translation.map(go => g.sign + go)
    }.map(_.getOrElse("OOPS"))

    symbolSignature
    
  }
}