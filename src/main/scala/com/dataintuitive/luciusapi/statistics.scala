package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver._
import functions.StatisticsFunctions

object statistics extends SparkJob with NamedObjectSupport with Globals {

  import Common._

  // No validation required here, except maybe the existence of the RDD
  // TODO
  override def validate(sc: SparkContext, config: Config): SparkJobValidation =
    SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Arguments for endpoint functions
    val input = (db, genes)
    val parameters = null

    Map(
      "info"   -> StatisticsFunctions.info(input, parameters),
      "header" -> StatisticsFunctions.header(input, parameters),
      "data"   -> StatisticsFunctions.statistics(input, parameters)
    )
  }

}
