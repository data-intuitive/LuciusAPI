package com.dataintuitive.luciusapi

import functions.ZhangFunctions._

import com.typesafe.config.Config
import org.apache.spark._
import scala.util.Try
import spark.jobserver.{NamedRddSupport, SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

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
object zhang extends SparkJob with NamedRddSupport with Globals {

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
    val signatureQuery:String = Try(config.getString("query")).getOrElse("")
    val rawSignature = signatureQuery.split(" ")
    // Sort the similarities?
    val sortResult:Boolean = Try(config.getString("sorted").toBoolean).getOrElse(false)

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint functions
    val input = (db, genes)
    val parameters = (rawSignature, sortResult)

    version match {
      case "v2" => Map(
        "info"   -> info(input, parameters),
        "header" -> header(input, parameters),
        "data"   -> result(input, parameters)
      )
      case _ => result(input, parameters).map(_.values.toList)
    }

  }

}