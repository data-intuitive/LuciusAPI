package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore._
import genes._
import api._

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
  * Returns the binned Zhang/similarity scores for the whole database for a given query.
  *
  * Remark: Make sure you check the signature first, through checkSignature
  */
object binnedZhang extends SparkSessionJob with NamedObjectSupport {

  import BinnedCorrelation._

  type JobData = BinnedCorrelation.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val signature = paramSignature(config)
    val binsX = optParamBinsX(config, 20)
    val binsY = optParamBinsY(config, 20)
    val filtersParam = optParamFilters(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(signature) { SpecificData(_, binsX, binsY, filtersParam) }

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
