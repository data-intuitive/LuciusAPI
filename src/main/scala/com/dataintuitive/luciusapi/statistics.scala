package com.dataintuitive.luciusapi

// Functions implementation and common code
import functions.StatisticsFunctions._
import Common._

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
  * Return relevant metrics and information about the dataset.
  *
  * No input is required, the cached version of the the database is used.
  */
object statistics extends SparkSessionJob with NamedObjectSupport {

  type JobData = functions.StatisticsFunctions.JobData
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)

    withGood(db, genes) { JobData(_, _) }

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
