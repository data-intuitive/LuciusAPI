package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try

/**
  * Returns a list of compounds and corresponding samples matching a query, optionally with a limit on the number of results.
  *
  * Input:
  *
  * - __`query`__: Depending on the pattern, a regexp match or `startsWith` is applied (mandatory)
  *
  * - __`version`__: v1, v2 or t1 (optional, default is `v1`)
  *
  * - __`limit`__: The result size is limited to this number (optional, default is 10)
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
    s"""Returns a list of compounds and corresponding samples matching a query, optionally with a limit on the number of results.
      |
      | Input:
      | - query: Depending on the pattern, a regexp match or `startsWith` is applied (mandatory)
      | - version: v1, v2 or t1 (optional, default is `v1`)
      | - limit: The result size is limited to this number (optional, default is 10)
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
    val limit:Int = Try(config.getString("limit").toInt).getOrElse(10)

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
        .map(sample => (sample.compoundAnnotations.compound.getJnjs, sample.compoundAnnotations.compound.getName, sample.sampleAnnotations.sample.getPwid))

    val resultRDDasMap = resultRDD
        .map{case (jnjs, name, pwid) =>
          Map("jnjs" -> jnjs,
              "name" -> name,
              "pwid" -> pwid)
        }

    val resultRDDv1 = resultRDD
      .map{case (jnjs, name, pwid) => (jnjs, pwid) }


    val limitOutput = (resultRDD.count > limit)

    (version, !limitOutput) match {
      // v2: Return information about what is returned as well
      case ("v2", true)   =>  Map(
                                  "info"   -> s"Result for query $compoundQuery",
                                  "header" -> ("jnjs", "name", "pwid"),
                                  "data"   -> resultRDDasMap.collect
                              )
      case ("v2", false)  => Map(
                                  "info" -> s"First $limit matches for query $compoundQuery",
                                  "header" -> ("jnjs", "name", "pwid"),
                                  "data" -> resultRDDasMap.take(limit)
                              )
      case (_   , true)   => resultRDDv1.collect
      case (_   , false)  => resultRDDv1.take(limit)
    }
  }
}