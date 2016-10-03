package com.dataintuitive.luciusapi

import com.dataintuitive.test.BaseSparkContextSpec
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark._
import preprocess._
import org.scalatest.FlatSpec
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

/**
  * Tests for the `preprocess` endpoint.
  *
  * TODO: Find a more general approach to this, where a combination of tests is performed automatically.
  */
class preprocessTest extends FlatSpec with BaseSparkContextSpec {

  // Init

  val baseConfig = ConfigFactory.load()

  info("Test validation")

  "Validation" should "return no errors if all parameters provided" in {

    val thisConfigValid = baseConfig
      .withValue("locationFrom", ConfigValueFactory.fromAnyRef("locFrom"))
      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
      .withValue("version", ConfigValueFactory.fromAnyRef("v1"))

    assert(preprocess.validate(sc, thisConfigValid) === SparkJobValid)

  }

  "Validation" should "return appropriate errors with no From" in {

    val thisConfigNoFrom = baseConfig
      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
      .withValue("version", ConfigValueFactory.fromAnyRef("v1"))
    val expectedErrorNoFrom = "locationFrom not defined in POST config"

    assert(preprocess.validate(sc, thisConfigNoFrom) === SparkJobInvalid(expectedErrorNoFrom))

  }

  "Validation" should "return appropriate errors with incorrect version" in {

    val thisConfigVersion = baseConfig
      .withValue("locationFrom", ConfigValueFactory.fromAnyRef("locFrom"))
      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
      .withValue("version", ConfigValueFactory.fromAnyRef("v3"))
    val expectedErrorNoVersion = "version should be either v1 or v2"

    assert(preprocess.validate(sc, thisConfigVersion) === SparkJobInvalid(expectedErrorNoVersion))

  }

  info("Test run")

  val configBlob =
    """
      | {
      |   locationFrom = "file://v1"
      |   locationTo = "file://processed/"
      | }
    """.stripMargin

  "runJob" should "run and return correct result" in {

    // TODO: Test if v1 and v2 formats are covered properly.
    // Problem is: I have no v2 with public data and even v1 is not yet in shape...

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    println(thisConfig.root().render())


  }

}
