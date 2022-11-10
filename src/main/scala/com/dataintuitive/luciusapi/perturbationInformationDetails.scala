package com.dataintuitive.luciusapi

import com.dataintuitive.luciusapi.Common.ParamHandlers._
import com.dataintuitive.luciuscore._
import api.v4_1._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalactic.Accumulation._
import org.scalactic._
import spark.jobserver._
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

/**
  * Returns the information of a certain specified perturbation.
  *
  * Input:
  *
  * - __`query`__: The perturbation to retrieve the information of
  */
object perturbationInformationDetails extends SparkSessionJob with NamedObjectSupport {

  import PerturbationsInformationDetails._

  type JobData = PerturbationsInformationDetails.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val perturbationQuery = paramPerturbation(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(perturbationQuery) { SpecificData(_) }

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
