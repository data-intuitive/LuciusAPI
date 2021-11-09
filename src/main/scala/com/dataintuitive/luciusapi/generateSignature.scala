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

object generateSignature extends SparkSessionJob with NamedObjectSupport {

  import GenerateSignature._

  type JobData = GenerateSignature.JobData
  type JobOutput = Array[String]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val version = validVersion(config)
    val db = getDB(runtime)
    val flatDb = getFlatDB(runtime)
    val genes = getGenes(runtime)
    val filters = getFilters(runtime)

    val pValue = optPValue(config)
    val perturbations = paramSamples(config)

    val cachedData = withGood(db, flatDb, genes, filters) { CachedData(_, _, _, _) }
    val specificData = withGood(perturbations) { SpecificData(pValue, _) }

    withGood(version, cachedData, specificData) { JobData(_, _, _) }
  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    result(data)

  }

}
