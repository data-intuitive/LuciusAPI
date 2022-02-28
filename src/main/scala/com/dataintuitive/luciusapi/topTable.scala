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

object topTable extends SparkSessionJob with NamedObjectSupport {

  import TopTable._

  type JobData = TopTable.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val head = optParamHead(config)
    val tail = optParamTail(config)
    val signature = optParamSignature(config)
    val features = optParamFeatures(config)
    val filtersParam = optParamFilters(config)

    val isValidHeadTail = validHeadTail(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(isValidHeadTail) { _ => SpecificData(head, tail, signature, features, filtersParam) }

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
