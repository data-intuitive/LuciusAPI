package com.dataintuitive.luciusapi

import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.{SparkJobInvalid, SparkJobValid}

class zhangTest extends FunSpec with Matchers with InitBefore {

  import zhang._

  // Init

  val baseConfig = ConfigFactory.load()

  describe("Zhang") {

    it("Should return help message when asked for - 'help=true'") {

      val configBlob =
        """
          | { help = true
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be (SparkJobInvalid(helpMsg))
    }

    it("Should return correct form and content") {

      // v2 interface
      val configBlob =
        """
          | {
          |   query = "MELK BRCA1"
          |   sorted = true
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
    
  }

}
