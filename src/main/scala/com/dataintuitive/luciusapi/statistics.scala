package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver._

import scala.util.Try


/**
  * Created by toni on 04/10/16.
  */
object statistics extends SparkJob with NamedObjectSupport with Globals {

  import Common._
  import functions.StatisticsFunctions

  type Output = Map[String, Any]
  type OutputData = Seq[(String, Any)]

  // No validation required here, except maybe the existence of the RDD
  // TODO
  override def validate(sc: SparkContext, config: Config): SparkJobValidation =
    SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // Configuration for this endpoint
    val config:StatisticsFunctions.Config = (db, genes)

    // Run endpoint function
    val outputInfo   = StatisticsFunctions.info(config)
    val outputHeader = StatisticsFunctions.header(config)
    val outputData   = StatisticsFunctions.statistics(config)

    Map(
      "info"   -> "General statistics about the data",
      "header" -> outputHeader,
      "data"   -> outputData
    )
  }


}
