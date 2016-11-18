package com.dataintuitive.luciusapi

import spark.jobserver.{SparkJobInvalid, SparkJobValid}
import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

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

  }

}
