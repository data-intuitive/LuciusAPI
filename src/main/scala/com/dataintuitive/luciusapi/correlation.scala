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
  */
object correlation extends SparkSessionJob with NamedObjectSupport {

  import Correlation._

  type JobData = Correlation.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val signature1 = paramSignature1(config)
    val signature2 = paramSignature2(config)
    val bins = optParamBins(config, 20)
    val filtersParam = optParamFilters(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(signature1, signature2) { SpecificData(_, _, bins, filtersParam) }

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
