package com.dataintuitive.luciusapi

// Functions implementation and common code
import com.dataintuitive.luciusapi.functions.TopTableFunctions._
import Common.ParamHandlers._


// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.genes._

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

object topTable extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.TopTableFunctions.JobData
  type JobOutput = Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val targets = paramTargets(config)
    val limit = optParamLimit(config)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)

    val signature = optParamSignature(config)
    val features = optParamFeatures(config)
    val filters = optParamFilters(config)

    val head = optParamHead(config)
    val tail = optParamTail(config)
    val isValidHeadTail = validHeadTail(config)

    (isValidVersion zip isValidHeadTail zip
      withGood(db, genes) {
        JobData(_, _, version, head, tail, signature, features, filters)
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
