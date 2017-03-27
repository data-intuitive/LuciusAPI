package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.functions.HistogramFunctions._
import com.dataintuitive.luciuscore.Model.DbRow
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try


object histogram extends SparkJob with NamedRddSupport with Globals {

  import Common._

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

    // sample query string
    val signatureString:String = Try(config.getString("query")).getOrElse(".*")
    val signatureSpecified = !(signatureString == ".*")
    val signatureQuery = signatureString.split(" ").toList

    // features to return: list of features
    val featuresString:String = Try(config.getString("features")).getOrElse("zhang")
    val featuresQueryWithoutZhang = featuresString.split(" ").toList
    // Always add zhang score wrt signature
    val featuresQuery = (featuresQueryWithoutZhang ++ List("zhang")).distinct

    // The number of bins
    val binsString:String = Try(config.getString("bins")).getOrElse("15")
    val nrBins = binsString.toInt

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
    val parameters = (version, signatureQuery, featuresQuery, nrBins, filters)

    Map(
      "info"   -> info(input, parameters),
      "header" -> header(input, parameters),
      "data"   -> result(input, parameters)
    )

  }



}