package com.dataintuitive.luciusapi

import spark.jobserver.{SparkJobInvalid, SparkJobValid}
import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

class compoundsTest extends FunSpec with Matchers with InitBefore {

  import compounds._

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

      validate(sc, thisConfig) should be (SparkJobInvalid(helpMsg))
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

      val expectedResult:OutputData =
          Array(("BRD-K01311880", "BRD-K01311880"))

      validate(sc, thisConfig) should be (SparkJobValid)

      // Specify type, Any does not result in a proper check
      val result = runJob(sc, thisConfig).asInstanceOf[Output]
      val outputData = result("data").asInstanceOf[OutputData]
      outputData should be (expectedResult)
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

      validate(sc, thisConfig) should be (SparkJobValid)

      val result = runJob(sc, thisConfig).asInstanceOf[Output]
      val outputData = result("data").asInstanceOf[OutputData]
      outputData.length should be > 0
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

      validate(sc, thisConfig) should be (SparkJobValid)
      runJob(sc, thisConfig).asInstanceOf[OutputData] should be (expectedResult)
    }

    it("Should return 10 compounds when large result set") {

      // v1 interface returns no "info"
      val configBlob =
      """
        | {
        |   version = v1
        |   query = ".*"
        | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobValid)

      val result = runJob(sc, thisConfig).asInstanceOf[OutputData]
      result.length should be (10)
    }

  it("result set is configurable") {

    // v1 interface returns no "info"
    val configBlob =
    """
      | {
      |   version = v1
      |   query = ".*"
      |   limit = 5
      | }
    """.stripMargin

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    validate(sc, thisConfig) should be (SparkJobValid)

    val result = runJob(sc, thisConfig).asInstanceOf[OutputData]
    result.length should be (5)
  }


  }

}
