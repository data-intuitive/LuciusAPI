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

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    // TODO

    SparkJobValid

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    (
      ("# samples"   -> db.count),
      ("# genes"     -> genes.genes.size),
      ("# compounds" -> db.map(_.compoundAnnotations.compound.name).distinct.count)
    )

  }


}
