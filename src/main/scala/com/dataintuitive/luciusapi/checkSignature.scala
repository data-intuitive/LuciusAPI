package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import scala.util.Try
import functions.CheckSignatureFunctions

/**
  * Returns annotations about genes (exists in l1000, symbol)
  *
  * Input:
  *
  * - __`query`__: a gene signature where genes can be in any format symbol, ensembl, probeset, entrez (mandatory)
  */
object checkSignature extends SparkJob with NamedRddSupport with Globals {

  import Common._

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
      case (true, _) => SparkJobInvalid(CheckSignatureFunctions.helpMsg)
      case (false, true) => SparkJobValid
      case (false, false) => SparkJobInvalid(allTests._2)
    }
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Compound query string
    val signatureQuery:String = Try(config.getString("query")).getOrElse("")
    val rawSignature = signatureQuery.split(" ").toList

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint functions
    val input = (db, genes)
    val parameters = rawSignature

    Map(
      "info"   -> CheckSignatureFunctions.info(input, parameters),
      "header" -> CheckSignatureFunctions.header(input, parameters),
      "data"   -> CheckSignatureFunctions.checkSignature(input, parameters)
    )

  }

}