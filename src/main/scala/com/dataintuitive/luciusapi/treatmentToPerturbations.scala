package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.Common.ParamHandlers._
import com.dataintuitive.luciuscore._
import com.dataintuitive.luciuscore.api._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalactic.Accumulation._
import org.scalactic._
import spark.jobserver._
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

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
object treatmentToPerturbations extends SparkSessionJob with NamedObjectSupport {

  import TreatmentToPerturbations._

  type JobData = TreatmentToPerturbations.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val pValue = optPValue(config, 0.05)
    val compoundQuery = paramCompounds(config)
    val limit = optParamLimit(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(compoundQuery) { SpecificData(pValue, _, limit) }

    withGood(version, cachedData, specificData) { JobData(_, _, _) }

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    data.version match {
      case "v2" =>
        Map(
          "info" -> infoMsg,
          "header" -> header(data),
          "data" -> result(data)
        )

      case _ => Map("result" -> result(data))
    }

  }

}