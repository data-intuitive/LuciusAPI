package com.dataintuitive.luciusapi

import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.{SparkJobInvalid, SparkJobValid}

class annotatedplatewellidsTest extends FunSpec with Matchers with InitBefore {

  import annotatedplatewellids._

  // Init

  val baseConfig = ConfigFactory.load()

  describe("Annotatedplatewellids") {

    it("Should return help message when asked for - 'help=true'") {

      val configBlob =
        """
          | { help = true
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobInvalid(helpMsg))
    }

    it("Should validate correctly") {

      // v2 interface
      val configBlob =
        """
          | {
          |   query = "MELK BRCA1"
          |   version = v2
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobValid)

    }

    it("Should return reasonable results") {

      // v2 interface
      val configBlob =
      """
        | {
        |   query = "MELK BRCA1"
        |   version = v2
        | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobValid)

      // Specify type, Any does not result in a proper check
      val result = runJob(sc, thisConfig).asInstanceOf[Output]
      val outputData = result("data").asInstanceOf[OutputData]

      outputData.length should be > (1)

      (outputData(0)._2 >= outputData(1)._2) should be (true)

    }

    it("Should limit results if a wildcard is provided") {

      // v2 interface
      val configBlob =
      """
        | {
        |   query = "MELK BRCA1"
        |   version = v2
        |   limit = 2
        | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobValid)

      // Specify type, Any does not result in a proper check
      val result = runJob(sc, thisConfig).asInstanceOf[Output]
      val outputData = result("data").asInstanceOf[OutputData]

      outputData.length should be  (2)

    }

    it("Should give zhang score 0.0 when no query provided") {

      // v2 interface
      val configBlob =
      """
        | {
        |   version = v2
        |   limit = 1
        | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobValid)

      // Specify type, Any does not result in a proper check
      val result = runJob(sc, thisConfig).asInstanceOf[Output]
      val outputData = result("data").asInstanceOf[OutputData]

      outputData.head._1 should be  (0.0)

    }

  }

}
