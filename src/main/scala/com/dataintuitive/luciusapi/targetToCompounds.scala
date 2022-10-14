package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore._
import genes._
import api.v4_1._

import Common.ParamHandlers._

import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver._

import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config

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

  import TargetToCompounds._

  type JobData = TargetToCompounds.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val targets = paramTargets(config)
    val limit = optParamLimit(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(targets) { SpecificData(_, limit) }

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
