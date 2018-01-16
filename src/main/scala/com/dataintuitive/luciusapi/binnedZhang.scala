package com.dataintuitive.luciusapi

// Functions implementation and common code
import com.dataintuitive.luciusapi.functions.BinnedZhangFunctions._
import Common.ParamHandlers._

// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.GeneModel._

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
  * Returns the binned Zhang/similarity scores for the whole database for a given query.
  *
  * Remark: Make sure you check the signature first, through checkSignature
  */
object binnedZhang extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.BinnedZhangFunctions.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val signature = paramSignature(config).map(_.toArray)
    val binsX = optParamBinsX(config, 20)
    val binsY = optParamBinsY(config, 20)
    val filters = optParamFilters(config)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)

    (isValidVersion zip
      withGood(db, genes, signature) {
        JobData(_, _, version, _, binsX, binsY, filters)
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
