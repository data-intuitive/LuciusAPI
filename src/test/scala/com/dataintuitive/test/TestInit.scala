package com.dataintuitive.test

import akka.actor.ActorSystem
import com.dataintuitive.jobserver.NamedDataSet
import com.dataintuitive.luciusapi.Common.{DataSetPersister, broadcastPersister}
import com.dataintuitive.luciusapi.initialize
import com.dataintuitive.luciusapi.initialize.JobData
import com.dataintuitive.luciuscore.api.FlatDbRow
import com.dataintuitive.luciuscore.genes.{Gene, GenesDB}
import com.dataintuitive.luciuscore.model.v4.Perturbation
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import spark.jobserver.api.JobEnvironment
import spark.jobserver.{JobServerNamedObjects, NamedBroadcast, NamedObjectSupport, NamedObjects}


object BaseSparkContextSpec {

  lazy val sc = new SparkContext("local[2]", getClass.getSimpleName, new SparkConf)
  lazy val sparkSession: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
  sc.setLogLevel("ERROR")

  lazy val jobEnvironment = new JobEnvironment {
    override val jobId: String = "1"
    override val namedObjects: NamedObjects = new JobServerNamedObjects(ActorSystem("NamedObjectsSpec"))
    override val contextConfig: Config = null
  }

}


trait BaseSparkContextSpec {

  def sparkSession = BaseSparkContextSpec.sparkSession
  def runtime = BaseSparkContextSpec.jobEnvironment

}

trait InitBefore extends Suite with BaseSparkContextSpec with BeforeAndAfterAll with NamedObjectSupport  { this: Suite =>

    override def beforeAll() {
      runtime.namedObjects.getNames().foreach { runtime.namedObjects.forget }

      val baseConfig = ConfigFactory.load()

      val configBlob =
        """
          | {
          |  db.uri = "src/test/resources/processed/testData.parquet"
          |  geneAnnotations = "src/test/resources/geneAnnotations.txt"
          | }
      """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val jobData = initialize.validate(sparkSession, BaseSparkContextSpec.jobEnvironment, thisConfig)
      fakeInitializeRunJob(sparkSession, BaseSparkContextSpec.jobEnvironment, jobData.get)

    }

  def fakeInitializeRunJob(sparkSession: SparkSession,
                 runtime: JobEnvironment,
                 data: JobData): Unit = {

    import sparkSession.implicits._

    // Create empty genes database
    val genesBC = sparkSession.sparkContext.broadcast(GenesDB(Array[Gene]()))
    runtime.namedObjects.update("genes", NamedBroadcast(genesBC))

    // Create empty perturbation database
    val schema_rdd = Encoders.product[Perturbation].schema
    val db = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema_rdd).as[Perturbation]
    val dbNamedDataset = NamedDataSet[Perturbation](db, forceComputation = false, storageLevel = data.storageLevel)
    runtime.namedObjects.update("db", dbNamedDataset)

    // Pretty original code to create flatDB, but will be quite empty too
    val flatDb = db.map( row =>
      FlatDbRow(
        row.id,
        row.info.cell.getOrElse("N/A"),
        row.trt.trt_cp.map(_.dose).getOrElse("N/A"),
        row.trtType,
        row.trt.trt.name,
        row.profiles.profile.map(_.p.map(_.count(_ <= 0.05)).getOrElse(0) > 0).getOrElse(false)
      )
    )

    val flatDbNamedDataset = NamedDataSet[FlatDbRow](flatDb, forceComputation = false, storageLevel = data.storageLevel)
    runtime.namedObjects.update("flatdb", flatDbNamedDataset)
  }

}