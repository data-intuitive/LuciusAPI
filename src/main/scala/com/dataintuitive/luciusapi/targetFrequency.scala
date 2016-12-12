package com.dataintuitive.luciusapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scala.util.Try

object targetFrequency extends SparkJob with NamedRddSupport with Globals {

  import Common._

  val simpleChecks:SingleParValidations = Seq()

  val combinedChecks:CombinedParValidations = Seq()

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    SparkJobValid

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Input
    val pwidsString:String = Try(config.getString("pwids")).getOrElse(".*")
    // Are specific pwids specified?
    val pwidsSpecified = !(pwidsString == ".*")
    val pwidsQuery = pwidsString.split(" ").toList

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    val targets =
          db.flatMap{ x =>
            if (pwidsQuery.contains(x.pwid.get))
              x.compoundAnnotations.knownTargets.map(_.toList.map(target => Some((target,x.pwid.get))))
            else
              None
          }.flatMap(x => x.get)

    val grouped = targets.groupByKey
    val result = grouped.map{case(gene, targetList) => (gene, targetList.size)}
//
    result.collect.sortBy{case(target, count) => -count}

  }

}


