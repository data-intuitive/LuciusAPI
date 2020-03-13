package com.dataintuitive.luciusapi

// Functions implementation and common code
import functions.CheckSignatureFunctions._
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
  * Returns annotations about genes (exists in l1000, symbol)
  *
  * Input:
  *
  * - __`query`__: a gene signature where genes can be in any format symbol, ensembl, probeset, entrez (mandatory)
  */
object checkSignature extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.CheckSignatureFunctions.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val signature = optParamSignature(config)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)

    (isValidVersion zip
      withGood(db, genes) {
        JobData(_, _, version, signature)
      }).map(_._2)

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    Map(
      "info" -> info(data),
      "header" -> header(data),
      "data" -> result(data)
    )

  }

}
