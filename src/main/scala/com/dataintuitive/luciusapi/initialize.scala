package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.Model.OldDbRow
import com.dataintuitive.luciuscore.io.GenesIO._
import com.dataintuitive.luciuscore.io.IoFunctions._
import com.typesafe.config.Config

import Common.ParamHandlers._

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import spark.jobserver.SparkSessionJob
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import org.apache.spark.sql.Dataset

import scala.util.Try
import org.scalactic._
import Accumulation._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import spark.jobserver._

import Common._
import com.dataintuitive.luciusapi.Model.FlatDbRow

import com.dataintuitive.jobserver.NamedDataSet
import com.dataintuitive.jobserver.DataSetPersister

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object CoreFuncions {

}
/**
  * Initialize the API by caching the database with sample-compound information
  *
  * We make a distinction between running within SparkJobserver and not.
  * The difference is in the handling of named objects in-between api calls.
  *
  * - For Jobserver we use NamedObject support for both the RDD and the dictionary of genes.
  * - For local, we use PersistentRDDs in combination with a new loading of the genes database at every call.
  */
object initialize extends SparkSessionJob with NamedObjectSupport {

  case class JobData(db: String,
                     geneAnnotations: String,
                     dbVersion: String,
                     partitions: Int,
                     storageLevel: StorageLevel,
                     geneFeatures: Map[String, String])
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = paramDb(config)
    val genes = paramGenes(config)
    val dbVersion = paramDbVersion(config)
    val partitions = paramPartitions(config)
    val storageLevel = paramStorageLevel(config)
    val geneFeatures = paramGeneFeatures(config)

    withGood(db, genes) { JobData(_, _, dbVersion, partitions, storageLevel, geneFeatures) }

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    import sparkSession.implicits._
    // implicit def DataSetPersister[T]: NamedObjectPersister[NamedDataSet[T]] =
    //   new DataSetPersister[T]
    // implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] =
    //   new BroadcastPersister[U]

    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", data.partitions.toString)

    // Backward compatibility
    val fs_s3_awsAccessKeyId = sys.env
      .get("AWS_ACCESS_KEY_ID")
      .getOrElse("<MAKE SURE KEYS ARE EXPORTED>")
    val fs_s3_awsSecretAccessKey =
      sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("<THE SAME>")
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", fs_s3_awsAccessKeyId)
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3n.awsSecretAccessKey", fs_s3_awsSecretAccessKey)

    // Loading gene annotations and broadcast
    val genes =
      loadGenesFromFile(sparkSession.sparkContext, data.geneAnnotations, delimiter="\t", dict = data.geneFeatures)
    val genesDB = new GenesDB(genes)
    val genesBC = sparkSession.sparkContext.broadcast(genesDB)

    runtime.namedObjects.update("genes", NamedBroadcast(genesBC))

    // Load data
    val parquet = sparkSession.read
      .parquet(data.db)
    val dbRaw = (parquet, data.dbVersion) match {
      case (oldFormatParquet, "v1") => oldFormatParquet.as[OldDbRow].map(_.toDbRow)
      case (newFormatParquet, _)    => newFormatParquet.as[DbRow]
    }
    val db = dbRaw.repartition(data.partitions)

    val dbNamedDataset = NamedDataSet[DbRow](db, forceComputation = true, storageLevel = data.storageLevel)

    runtime.namedObjects.update("db", dbNamedDataset)

    val flatDb = db.map( row =>
      FlatDbRow(
        row.id.getOrElse("NA"),
        row.sampleAnnotations.sample.getProtocolname,
        row.compoundAnnotations.compound.getJnjs,
        row.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0) > 0
      )
    )

    val flatDbNamedDataset = NamedDataSet[FlatDbRow](flatDb, forceComputation = true, storageLevel = data.storageLevel)

    runtime.namedObjects.update("flatdb", flatDbNamedDataset)

    Map(
      "info" -> "Initialization done",
      "header" -> "None",
      "data" -> (db.rdd.count, flatDb.rdd.count, genes)
    )
  }

}

