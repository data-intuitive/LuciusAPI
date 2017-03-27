package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.functions.BinnedZhangFunctions._
import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver._

import scala.util.Try

/**
  * Returns the Zhang/similarity scores for the whole database for a given query.
  *
  * Input:
  *
  * - __`query`__: The gene signature (any notation will do) as a space separated list
  *
  * - __`sorted`__: Should the result be sorted on by Zhang score (descending)
  *
  * - __`version`__: Version of the API. "v2" returns `info` and `header`.
  *
  * Remark: Make sure you check the signature first, through checkSignature
  */
object binnedZhang extends SparkJob with NamedRddSupport with Globals {

  import Common._

  val simpleChecks:SingleParValidations = Seq(
    ("query",   (isDefined ,    "query not defined in POST config")),
    ("query",   (isNotEmpty ,   "query is empty in POST config"))
  )

  val combinedChecks:CombinedParValidations = Seq()

// TODO: sorted = boolean !
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

    // API Version
    val version = Try(config.getString("version")).getOrElse("v1")
    // Compound query string
    val signatureString:String = Try(config.getString("query")).getOrElse("")
    val signatureQuery = signatureString.split(" ")
    // Sort the similarities?
//    val sortResult:Boolean = Try(config.getString("sorted").toBoolean).getOrElse(false)

    // Bins in x and y
    val binsX = Try(config.getString("binsX").toInt).getOrElse(20)
    val binsY = Try(config.getString("binsY").toInt).getOrElse(20)

    // Filters
    val filterConcentrationString:String = Try(config.getString("filter.concentration")).getOrElse("")
    val filterConcentrationQuery = filterConcentrationString
    val filterTypeString:String = Try(config.getString("filter.type")).getOrElse("")
    val filterTypeQuery = filterTypeString
    val filterProtocolString:String = Try(config.getString("filter.protocol")).getOrElse("")
    val filterProtocolQuery = filterProtocolString

    val filters = Map(
      "concentration" -> filterConcentrationQuery,
      "type" -> filterTypeQuery,
      "protocol" -> filterProtocolQuery
    )

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint functions
    val input = (db, genes)
    val parameters = (signatureQuery, binsX, binsY, filters)

    Map(
        "info"   -> info(input, parameters),
        "header" -> header(input, parameters),
        "data"   -> result(input, parameters)
      )

  }

}