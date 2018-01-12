package com.dataintuitive.luciusapi

// Functions implementation and common code
import functions.TargetToCompoundsFunctions._
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

/**
  * Returns a list of compounds and corresponding samples matching a query, optionally with a limit on the number of results.
  *
  * Input:
  *
  * - __`query`__: List of targets to query 
  *
  * - __`version`__: v1, v2 or t1 (optional, default is `v2`)
  *
  * - __`limit`__: The result size is limited to this number (optional, default is 10)
  */
object targetToCompounds extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.TargetToCompoundsFunctions.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val targets = paramTargets(config)
    val limit = optParamLimit(config)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)

    (isValidVersion zip
      withGood(db, genes, targets) {
        JobData(_, _, version, _, limit)
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
