package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.test.BaseSparkContextSpec
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

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
      .withValue("sampleCompoundRelations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("geneAnnotations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("tStats", ConfigValueFactory.fromAnyRef("x"))
      .withValue("pStats", ConfigValueFactory.fromAnyRef("x"))

    assert(preprocess.validate(sc, thisConfigValid) === SparkJobValid)

  }

  it should "return appropriate errors with no From" in {

    val thisConfigNoFrom = baseConfig
      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
      .withValue("version", ConfigValueFactory.fromAnyRef("v1"))
      .withValue("sampleCompoundRelations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("geneAnnotations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("tStats", ConfigValueFactory.fromAnyRef("x"))
      .withValue("pStats", ConfigValueFactory.fromAnyRef("x"))
    val expectedErrorNoFrom = "locationFrom not defined in POST config"

    assert(preprocess.validate(sc, thisConfigNoFrom) === SparkJobInvalid(expectedErrorNoFrom))

  }

  it should "return appropriate errors with multiple missing values" in {

    val thisConfigNoMult = baseConfig
      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
      .withValue("version", ConfigValueFactory.fromAnyRef("v1"))
      .withValue("geneAnnotations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("tStats", ConfigValueFactory.fromAnyRef("x"))
      .withValue("pStats", ConfigValueFactory.fromAnyRef("x"))
    val expectedErrorNoMult = "locationFrom not defined in POST config, sampleCompoundRelations not defined in POST config"

    assert(preprocess.validate(sc, thisConfigNoMult) === SparkJobInvalid(expectedErrorNoMult))

  }

  it should "return appropriate errors with incorrect version" in {

    val thisConfigVersion = baseConfig
      .withValue("locationFrom", ConfigValueFactory.fromAnyRef("locFrom"))
      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
      .withValue("version", ConfigValueFactory.fromAnyRef("v3"))
      .withValue("sampleCompoundRelations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("geneAnnotations", ConfigValueFactory.fromAnyRef("x"))
      .withValue("tStats", ConfigValueFactory.fromAnyRef("x"))
      .withValue("pStats", ConfigValueFactory.fromAnyRef("x"))
    val expectedErrorNoVersion = "version should be either v1, v2 or t1"

    assert(preprocess.validate(sc, thisConfigVersion) === SparkJobInvalid(expectedErrorNoVersion))

  }

  info("Test run")

  val configBlob =
    """
      | {
      |   version = "t1"
      |   locationFrom = "src/test/resources/"
      |   sampleCompoundRelations = "sampleCompoundRelations.txt"
      |   geneAnnotations = "geneAnnotations.txt"
      |   tStats = "tStats-transposed-head.txt"
      |   pStats = "pStats-transposed-head.txt"
      |   locationTo = "src/test/resources/processed/"
      | }
    """.stripMargin

  "runJob" should "run and return correct result" in {

    // TODO: Test if v1 and v2 formats are covered properly.
    // Problem is: I have no v2 with public data and even v1 is not yet in shape...

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    assert(preprocess.validate(sc, thisConfig) === SparkJobValid)

    val fileExists = Try(sc.textFile("src/test/resources/processed/db")).toOption.isDefined
    if (!fileExists)
      preprocess.runJob(sc, thisConfig)

    val rdd:RDD[DbRow] = sc.objectFile("src/test/resources/processed/db")
    val firstEntry = rdd.first

    assert(firstEntry.pwid.isDefined)

  }

}
