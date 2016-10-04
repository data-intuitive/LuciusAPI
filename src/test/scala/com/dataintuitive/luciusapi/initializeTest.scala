package com.dataintuitive.luciusapi

import com.dataintuitive.test.BaseSparkContextSpec
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.scalatest._
import spark.jobserver.JobServerNamedObjects

import scala.util.Try

/**
  * Tests for the `preprocess` endpoint.
  *
  * TODO: Find a more general approach to this, where a combination of tests is performed automatically.
  */
class initializeTest extends FlatSpec with BaseSparkContextSpec {

  // Init

  val baseConfig = ConfigFactory.load()

  info("Test validation")

  "Validation" should "return no errors if all parameters provided" in {

    // TODO: Fill in

  }

  info("Test runJob")

  val configBlob =
    """
      | {
      |   location = "src/test/resources/processed/"
      |   geneAnnotations = "geneAnnotations.txt"
      | }
    """.stripMargin

  "runJob" should "run and return correct result" in {

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

   //    assert(preprocess.validate(sc, thisConfig) === SparkJobValid)

    val runJobResult = initialize.runJob(sc, thisConfig)

    // Collect the first element from the persisted/named RDD
    val firstElement = sc.getPersistentRDDs.map{case (index, rdd) => rdd}.filter(rdd => (rdd.name == "db")).head.asInstanceOf[RDD[DbRow]].first

    // Check if the first element is defined
    assert(firstElement.pwid.isDefined)

  }

}
