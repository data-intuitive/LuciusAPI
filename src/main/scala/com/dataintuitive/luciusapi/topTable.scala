package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.functions.TopTableFunctions._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try

object topTable extends SparkJob with NamedRddSupport with Globals {

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

    // Put a limit to the number of results, only if no pwids specified
    val head:Int = Try(config.getString("head").toInt).getOrElse(0)
    val tail:Int = Try(config.getString("tail").toInt).getOrElse(0)

    // features to return: list of features
    val featuresString:String = Try(config.getString("features")).getOrElse(".*")
    val featuresSpecified = !(featuresString == ".*")
    val featuresQuery = featuresString.split(" ").toList

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

    // Arguments for endpoint function
    val input = (db, genes)
    val parameters = (version, head, tail, signatureQuery, featuresQuery, filters)

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