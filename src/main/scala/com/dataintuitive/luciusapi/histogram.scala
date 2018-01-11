package com.dataintuitive.luciusapi

// Functions implementation and common code
import com.dataintuitive.luciusapi.functions.HistogramFunctions._
import Common._

// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.GeneModel._

// Jobserver
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver._

// Scala, Scalactic and Typesafe
import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object histogram extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.HistogramFunctions.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)
    val signature = paramSignature(config)
    val bins = optParamBins(config, 15)
    val features = optParamFeatures(config)
    val filters = optParamFilters(config)

    // features to return: list of features
    // val featuresString:String = Try(config.getString("features")).getOrElse("zhang")
    // val featuresQueryWithoutZhang = featuresString.split(" ").toList
    // // Always add zhang score wrt signature
    // val featuresQuery = (featuresQueryWithoutZhang ++ List("zhang")).distinct

    (isValidVersion zip
      withGood(db, genes, signature) {
        JobData(_, _, version, _, features, bins, filters)
      }).map(_._2)

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    data.version match {
      case "v2" =>
        Map(
          "info" -> info(data),
          "header" -> header(data),
          "data" -> result(data)
        )

      case _ => Map("result" -> result(data))
    }

  }

}
