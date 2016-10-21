package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.collection.MapLike
import scala.util.Try

/**
  * Returns annotations about genes (exists in l1000, symbol)
  *
  * Input:
  *
  * - __`query`__: a gene signature where genes can be in any format symbol, ensembl, probeset, entrez (mandatory)
  */
object checkSignature extends SparkJob with NamedRddSupport with Globals {

  import Common._

  type Output = Map[String, Any]
  type OutputData = Seq[(String, Boolean, String)]

  val helpMsg =
    s"""Returns annotations about genes (exists in l1000, symbol).
     |
     |Input:
     |- __`query`__: a gene signature where genes can be in any format symbol, ensembl, probeset, entrez (mandatory)
     """.stripMargin

  val simpleChecks:SingleParValidations = Seq(
    ("query",   (isDefined ,    "query not defined in POST config")),
    ("query",   (isNotEmpty ,   "query is empty in POST config"))
  )

  val combinedChecks:CombinedParValidations = Seq()


  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val showHelp = Try(config.getString("help")).toOption.isDefined
    val testsSingle = runSingleParValidations(simpleChecks, config)
    val testsCombined = runCombinedParValidations(combinedChecks, config)
    val allTests = aggregateValidations(testsSingle ++ testsCombined)

    (showHelp, allTests._1) match {
      case (true, _) => SparkJobInvalid(helpMsg)
      case (false, true) => SparkJobValid
      case (false, false) => SparkJobInvalid(allTests._2)
    }
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Compound query string
    val signatureQuery:String = Try(config.getString("query")).getOrElse("")
    val rawSignature = signatureQuery.split(" ")

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Create dictionary based on l1000 genes
    val probesetid2symbol = genes.genes.map(x => (x.probesetid, x.symbol)).toMap
    val ensemblid2symbol  = genes.genes.map(x => (x.ensemblid, x.symbol)).toMap
    val entrezid2symbol   = genes.genes.map(x => (x.entrezid, x.symbol)).toMap
    val symbol2symbol   = genes.genes.map(x => (x.symbol, x.symbol)).toMap

    val tt = probesetid2symbol ++ ensemblid2symbol ++ entrezid2symbol ++ symbol2symbol

    val l1000OrNot:OutputData = rawSignature
                      .map(gene => (gene, tt.get(gene)))
                      .map{case (gene, optionTranslation) =>
                        (gene, optionTranslation.isDefined, tt.getOrElse(gene,""))}

    val infoString = s"Signature of length ${l1000OrNot.length} contains ${l1000OrNot.count(_._2)} l1000 genes"

    Map(
      "info"   -> infoString,
      "header" -> Seq("gene","l1000?","symbol"),
      "data"   -> l1000OrNot
    )

  }

}