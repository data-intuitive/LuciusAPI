package com.dataintuitive.luciusapi

import spark.jobserver.{SparkJobInvalid, SparkJobValid}
import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

/**
  * Tests for the `preprocess` endpoint.
  *
  * TODO: Find a more general approach to this, where a combination of tests is performed automatically.
  */
class compoundsTest extends FunSpec with Matchers with InitBefore {

  // Init

  val baseConfig = ConfigFactory.load()

  describe("Compounds") {

    it("Should return help message when asked for - 'help=true'") {

      val configBlob =
        """
          | { help = true
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      compounds.validate(sc, thisConfig) should be (SparkJobInvalid(compounds.helpMsg))
    }

    it("Should return the correct result - v2") {

      // v2 interface
      val configBlob =
        """
          | {
          |   query = "BRD-K01311880"
          |   version = v2
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val expectedResult =
        Map(
          "info" -> "Result for query BRD-K01311880",
          "data" -> Array(("BRD-K01311880", "BRD-K01311880"))
      )

      compounds.validate(sc, thisConfig) should be (SparkJobValid)

      // Specify type, Any does not result in a proper check
      val result = compounds.runJob(sc, thisConfig).asInstanceOf[Map[String,Array[(String,String)]]]
      result.getOrElse("data", Array()) should be (expectedResult("data"))
    }

    it("Should match on regexp - v2") {

      // v2 interface
      val configBlob =
      """
        | {
        |   query = "BRD-K0131.*"
        |   version = v2
        | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      compounds.validate(sc, thisConfig) should be (SparkJobValid)

      val result = compounds.runJob(sc, thisConfig).asInstanceOf[Map[String,Array[(String,String)]]]
      result.getOrElse("data", Array()).length should be > 0
    }


    it("Should return the correct result - v1") {

      // v1 interface returns no "info"
      val configBlob =
        """
          | {
          |   query = "BRD-K01311880"
          |   version = v1
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val expectedResult = Array(("BRD-K01311880", "BRD-K01311880"))

      compounds.validate(sc, thisConfig) should be (SparkJobValid)
      compounds.runJob(sc, thisConfig) should be (expectedResult)
    }

    it("Should return 10 compounds when no query is given") {

      // v1 interface returns no "info"
      val configBlob =
      """
        | {
        |   version = v1
        | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      compounds.validate(sc, thisConfig) should be (SparkJobValid)
      val result = compounds.runJob(sc, thisConfig).asInstanceOf[Array[(String,String)]]
      result.length should be (10)
    }


  }

}
