package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try

/**
  * Given a regexp for a compound jnjs, returns a list of compounds and corresponding samples in the database matching the query.
  *
  * Please note that
  *
  * - if the result is larger than 100 entries, only the first 100 are shown.
  * - A `query` can be in two forms:
  *   1. A simple string: a match is done with compounds that start with this string
  *   2. A regexp: a match is done on the regexp
  */
object compounds extends SparkJob with NamedRddSupport with Globals {

  import Common._

  type Output = Map[String, Any]
  type OutputData = Array[(String, String)]

  // For backward compatibility, we do not check on version.
  val simpleChecks:SingleParValidations = Seq(
    ("query",   (isDefined ,    "query not defined in POST config")),
    ("query",   (isNotEmpty ,   "query is empty in POST config"))
  )

  val combinedChecks:CombinedParValidations = Seq()

  val helpMsg =
    s"""Given a regexp for a compound jnjs, returns a list of compounds and corresponding samples in the database matching the query.
        |
        | Options:
        |
        | - version: "v1" or "v2", depending on which version of the interface is required. (default = 'v1')
        | - query: regular expression matching the compounds jnjs (default = '.*')
       """.stripMargin

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
    val compoundQuery:String = Try(config.getString("query")).getOrElse(".*")

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Is could distinguish on version as well, but this makes more sense
    def isMatch(s: String, query:String):Boolean = {
      // Backward compatbility: Does query contains regexp or just first characters?
      val hasNonAlpha = compoundQuery.matches("^.*[^a-zA-Z0-9 ].*$")

      if (hasNonAlpha) s.matches(query)
      else s.startsWith(query)
    }

    val resultRDD =
      db
        .filter{sample => sample.compoundAnnotations.compound.jnjs.map(isMatch(_, compoundQuery)).getOrElse(false)}
        .map{sample => (sample.compoundAnnotations.compound.jnjs.getOrElse("NA"), sample.sampleAnnotations.sample.pwid.getOrElse("NA"))}

    val limitOutput = (resultRDD.count > 100)

    version match {
      // v1: Return just the tuples (jnjs, pwid)
      case "v1" =>  {
        if (!limitOutput) resultRDD.collect
        else resultRDD.take(100)
      }
      // v2: Return information about what is returned as well
      case "v2" =>  {
        if (!limitOutput) Map("info" -> s"Result for query $compoundQuery", "data" -> resultRDD.collect)
        else Map("info" -> "First 100 matches for all compounds ", "data" -> resultRDD.take(100))
      }
      // _: Falling back to v1
      case _    =>  {
        if (!limitOutput) resultRDD.collect
        else resultRDD.take(10)
      }
    }
  }
}