package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._


import scala.util.Try

/**
  * Given a a gene signature (symbol, ensembl, probeset, entrez), return if they are contained in L1000 or not.
  */
object checkSignature extends SparkJob with NamedRddSupport with Globals {

  import Common._

  val helpMsg =
    s"""Given a a gene signature (symbol, ensembl, probeset, entrez), return if they are contained in L1000 or not.
       |
       | Options:
       |
       | - query: Gene signature or list
       |
       """.stripMargin

  // This is ok if no combination of parameters are required
  val mandatoryConfigs:Seq[(String, (Option[String] => Boolean, String))] = Seq(
    ("query",   (isDefined ,    "query not defined in POST config")),
    ("query",   (isNotEmpty ,   "query is empty in POST config"))
  )

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val showHelp = Try(config.getString("help")).toOption.isDefined
    val allTests = runTests(mandatoryConfigs, config)

    showHelp match {
      case true => SparkJobInvalid(helpMsg)
      case false => {
        if (allTests._1) SparkJobValid
        else SparkJobInvalid(allTests._2)
      }
    }
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // API Version
    val version = Try(config.getString("version")).getOrElse("v1")
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

    val l1000OrNot = rawSignature
                      .map(gene => (gene, tt.get(gene)))
                      .map{case (gene, optionTranslation) => (gene, optionTranslation.isDefined)}

    val infoString = s"Signature of length ${l1000OrNot.length} contains ${l1000OrNot.count(_._2)} l1000 genes"

    Map(
      "info"    -> infoString,
      "data"    -> l1000OrNot,
      "l1000"   -> l1000OrNot.filter(_._2),
      "nol1000" -> l1000OrNot.filter(!_._2)
    )

  }

}