package com.dataintuitive.test

import akka.actor.ActorSystem
import com.dataintuitive.jobserver.NamedDataSet
import com.dataintuitive.luciusapi.initialize
import com.dataintuitive.luciusapi.initialize.{JobData, JobOutput}
import com.dataintuitive.luciuscore.api.FlatDbRow
import com.dataintuitive.luciuscore.genes.GenesDB
import com.dataintuitive.luciuscore.io.GenesIO.loadGenesFromFile
import com.dataintuitive.luciuscore.model.v4.Perturbation
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import spark.jobserver.{JobServerNamedObjects, NamedBroadcast, NamedObjectSupport, NamedObjects, SparkSessionJob}
import spark.jobserver.api.JobEnvironment






object BaseSparkContextSpec {

  lazy val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
  //lazy val sc = new SparkContext(conf)


  def getEnvironment(): JobEnvironment = {

    new JobEnvironment {
      override def jobId: String = "1"
      override def namedObjects: NamedObjects = new JobServerNamedObjects(ActorSystem("NamedObjectsSpec"))
      override def contextConfig: Config = null
    }
  }

  lazy val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  lazy val runtime: JobEnvironment = getEnvironment

}


trait BaseSparkContextSpec {

  //lazy val sc = BaseSparkContextSpec.sc
  //sc.setLogLevel("ERROR")

  lazy val sparkSession = BaseSparkContextSpec.sparkSession
  lazy val runtime = BaseSparkContextSpec.runtime

}

trait InitBefore extends Suite with BaseSparkContextSpec with BeforeAndAfterAll { this: Suite =>

    override def beforeAll() {

      val baseConfig = ConfigFactory.load()

      val configBlob =
        """
          | {
          |  db.uri = "src/test/resources/processed/testData.parquet"
          |  geneAnnotations = "src/test/resources/geneAnnotations.txt"
          | }
      """.stripMargin

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    /*
    Thread.sleep(5000)

    val runJobResult = initialize.runJob(sc, thisConfig)

    Thread.sleep(5000)
     */

    val jobData = initialize.validate(sparkSession, runtime, thisConfig)
    val dbNamedDataset = NamedDataSet[Perturbation](null, forceComputation = true, storageLevel = MEMORY_ONLY)
    //runtime.namedObjects.getOrElseCreate[NamedDataSet[Perturbation]]("db", null)


    //val jobOutput = initialize.runJob(sparkSession, runtime, jobData.get)




    }


}