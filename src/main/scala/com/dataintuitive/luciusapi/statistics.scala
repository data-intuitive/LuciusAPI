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
  * Return relevant metrics and information about the dataset.
  *
  * No input is required, the cached version of the the database is used.
  */
object statistics extends SparkSessionJob with NamedObjectSupport {

  import Statistics._

  type JobData = Statistics.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)

    val cachedData = withGood(db, flatDb, genes) { CachedData(_, _, _) }
    val specificData = SpecificData()

    withGood(version, cachedData) { JobData(_, _, specificData) }

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    Map(
      "info" -> infoMsg,
      "header" -> header(data),
      "data" -> result(data)
    )
  }

}
