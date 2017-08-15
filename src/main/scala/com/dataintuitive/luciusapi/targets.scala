package com.dataintuitive.luciusapi

import functions.TargetsFunctions._

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try

/**
 * Returns a list of targets based on a query, optionally with a limit on the number of results.
 * Input:
 * - query: `startsWith` query on available known targets (mandatory)
 * - version: v1, v2 or t1 (optional, default is `v1`)
 * - limit: The result size is limited to this number (optional, default is 10)
*/
object targets extends SparkJob with NamedRddSupport with Globals {

  import Common._

  // For backward compatibility, we do not check on version.
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

    // API Version, fallback to v1
    val version = Try(config.getString("version")).getOrElse("v1")
    // Targets query string, fallback to all targets (safe in combination with limit)
    val targetsQuery:String = Try(config.getString("query")).getOrElse(".*")
    // Limit on number of results, fallback is 10
    val limit:Int = Try(config.getString("limit").toInt).getOrElse(10)

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint function
    val input = (db, genes)
    val parameters = (version, targetsQuery, limit)

    version match {
      case "v2"   =>  Map(
                          "info"   -> info(input,parameters),
                          "header" -> header(input, parameters),
                          "data"   -> result(input, parameters)
                        )
      case _   => result(input, parameters)
    }
  }
}