package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.functions.AnnotatedplatewellidsFunctions._
import functions.AnnotatedplatewellidsFunctions._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try

object annotatedplatewellids extends SparkJob with NamedRddSupport with Globals {

  import Common._

  // TODO: check if platewellid endpoint is used, otherwise just make query mandatory for simplicity
  val simpleChecks:SingleParValidations = Seq()

  val combinedChecks:CombinedParValidations = Seq()

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val showHelp = Try(config.getString("help")).toOption.isDefined

    showHelp match {
      case true => SparkJobInvalid(helpMsg)
      case false => SparkJobValid
    }

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // API Version
    val version = Try(config.getString("version")).getOrElse("v1")

    // Gene query string
    val rawSignature:String = Try(config.getString("query")).getOrElse(".*")
    val signatureSpecified = !(rawSignature== ".*")
    val signatureQuery = rawSignature.split(" ").toList

    // pwids to filter on: list of regexps
    val pwidsString:String = Try(config.getString("pwids")).getOrElse(".*")
    val pwidsSpecified = !(pwidsString == ".*")
    val pwidsQuery = pwidsString.split(" ").toList

    // Put a limit to the number of results, only if no pwids specified
    val limit:Int = Try(config.getString("limit").toInt).getOrElse(10)

    // features to return: list of features
    val featuresString:String = Try(config.getString("features")).getOrElse(".*")
    val featuresSpecified = !(featuresString == ".*")
    val featuresQuery = featuresString.split(" ").toList

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint function
    val input = (db, genes)
    val parameters = (version, limit, signatureQuery, pwidsQuery, featuresQuery)

    version match {
      case "v2" =>  Map(
                      "info"   -> info(input, parameters),
                      "header" -> header(input, parameters),
                      "data"   -> result(input, parameters)
                    )

      case _    => result(input, parameters)
    }

  }

}