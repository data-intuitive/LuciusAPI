package com.dataintuitive.luciusapi

// Functions implementation and common code
import functions.CompoundToSamplesFunctions._
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
object compoundToSamples extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.CompoundToSamplesFunctions.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val limit = optParamLimit(config, 10)
    val pValue = optPValue(config, 0.05)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)
    val compoundQuery = paramCompounds(config)

    (isValidVersion zip
      withGood(db, genes, compoundQuery) {
        JobData(_, _, pValue, version, _, limit)
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
