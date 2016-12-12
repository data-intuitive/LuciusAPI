package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.functions.CheckSignatureFunctions._
import com.dataintuitive.luciuscore.Model.DbRow
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import com.dataintuitive.luciuscore.TransformationFunctions

import scala.util.Try


object signature extends SparkJob with NamedRddSupport with Globals {

  import Common._

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
    val compound:String = Try(config.getString("compound")).getOrElse("*")

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint functions
//    val input = (db, genes)
//    val parameters = signatureQuery

    // Create dictionary based on l1000 genes
    val probesetid2symbol = genes.genes.map(x => (x.probesetid, x.symbol))
    val ensemblid2symbol  = genes.genes.map(x => (x.ensemblid, x.symbol))
    val entrezid2symbol   = genes.genes.map(x => (x.entrezid, x.symbol))
    val symbol2symbol   = genes.genes.map(x => (x.symbol, x.symbol))

    val tt = probesetid2symbol ++ ensemblid2symbol ++ entrezid2symbol ++ symbol2symbol


    // Invert the translation table, but remove identities. This way, we show the gene symbols.
    // Be carefull, the translation_table should not be a map yet, because that way some of
    // the entries are removed!
    val itt = tt.map(_.swap).filter{case (x,y) => x != y}.toMap

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
    val rankVector = TransformationFunctions.stats2RankVector((valueVector,Array()))
    val indexSignature = TransformationFunctions.rankVector2IndexSignature(rankVector)

    val symbolDict = genes.symbol2ProbesetidDict
    val indexDict = genes.index2ProbesetidDict

    indexSignature
      .translate2Probesetid(indexDict)
      .translate2Symbol(symbolDict)


//    translateGenes(selectionOrderedSignature, itt)




//    Map(
//      "info"   -> info(input, parameters),
//      "header" -> header(input, parameters),
//      "data"   -> result(input, parameters)
//    )

  }

//  // Calculate median of a sequence of numbers
//  def median(l:Seq[Double]): Double = {
//    val lsorted = l.sorted
//    val length = l.size
//    if (length % 2 == 0) (lsorted.apply(length/2-1) + lsorted.apply(length/2))*1.0/2 else lsorted.apply(length/2)
//  }
//
//  // Calculate the intersection of a selection of compounds
//  // And retrieve the median values of the significant t-stats
//  // This is like the 'derived' expression vector for a set of compounds
//  def medianIntersection(selection:Array[DbRow]) = {
//    selection.
//      flatMap{        // Join datasets and add index for genes
//        x => x.sampleAnnotations.p.get.zip(x.sampleAnnotations.p.get).zipWithIndex.map{case ((p,t),i) => (i,(t, p))}
//      }
//      .groupBy{        // Group by gene
//        case(i,(t,p)) => i
//      }
//      .map{            // Select significant t-values, else 0.
//        case (i,a) => (i,a.map{case (j,(t,p)) => if (p<.05) t else 0.})
//      }
//      .map{            // Calculate median for the set of significant expressions
//        case (i,a) => (i, if (a.min == 0.0) 0. else median(a) )
//      }
//      .toArray.sorted.map(_._2)    // Make sure the result is sorted again.
//  }
//
//  // An implementation of this function that takes into account zero values.
//  // A better approach to converting a value vector into a rank vector.
//  // Be careful, ranks start at 1 in this implementation!
//  def valueVector2AvgRankVectorWithZeros(v: ValueVector): RankVector = {
//    // Helper function
//    def avg_unsigned_rank(ranks: RankVector): Double = {
//      ranks.foldLeft(0.)(+_ + _) / ranks.length
//    }
//
//    val zeros = v.filter(x => (x==0.)).length
//
//    v.zipWithIndex
//      .sortBy{ case(v,i) => Math.abs(v)}
//      .zipWithIndex                         // add an index, this becomes the rank
//      .sortBy{case ((v,i),j) => i}          // sort wrt original index
//      .map{case ((v,i),j) => ((v,i),j+1)}   // make sure ranks start at 1 rather than 0
//      .map{case ((v,i),j) => if (v==0.) ((v,i),0) else ((v,i),j-zeros)} // Make sure zero entries are not counted in rank
//      .map{ case((v,i),j) => Map[String, Double]("value" -> v, "orig_index" -> i, "unsigned_rank" -> j)}
//      .groupBy(x => Math.abs(x("value")))   // The specifics of the average rank calculation
//      .map(_._2)
//      .map(vector => vector.map(x => x ++ Map("avg_unsigned_rank" -> avg_unsigned_rank(vector.map(_("unsigned_rank"))))))
//      .flatMap(x => x).toList
//      .sortBy(_("orig_index"))
//      .map(x => if (x("value") >= 0) x("avg_unsigned_rank") else -x("avg_unsigned_rank"))
//      .toArray
//  }

}