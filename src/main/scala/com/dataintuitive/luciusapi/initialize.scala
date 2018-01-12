package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.GeneModel._
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.io.GenesIO
import com.typesafe.config.Config

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
// import spark.jobserver.NamedBroadcast
// import spark.jobserver.BroadcastPersister

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
                     partitions: Int,
                     storageLevel: StorageLevel)
  type JobOutput = collection.Map[String, Any]

  import Common._

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = paramDb(config)
    val genes = paramGenes(config)
    val partitions = paramPartitions(config)
    val storageLevel = paramStorageLevel(config)

    withGood(db, genes) { JobData(_, _, partitions, storageLevel) }

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    import sparkSession.implicits._
    implicit def DataSetPersister[T]: NamedObjectPersister[NamedDataSet[T]] =
      new DataSetPersister[T]
    implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] =
      new BroadcastPersister[U]

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
      GenesIO.loadGenesFromFile(sparkSession.sparkContext, data.geneAnnotations)
    val genesBC = sparkSession.sparkContext.broadcast(genes)
    runtime.namedObjects.update("genes", NamedBroadcast(genesBC))

    // Load data
    val db = sparkSession.read
      .parquet(data.db)
      .as[DbRow]
      .repartition(data.partitions)
    runtime.namedObjects.update(
      "db",
      NamedDataSet[DbRow](db, forceComputation = true, storageLevel = data.storageLevel))

    // "LuciusAPI initialized..."

    Map(
      "info" -> "Initialization done",
      "header" -> "None",
      "data" -> db.count
    )
  }

}

/**
  * wrapper for named objects of type DataFrame
  */
case class NamedDataSet[T](ds: Dataset[T], forceComputation: Boolean, storageLevel: StorageLevel)
    extends NamedObject

/**
  * implementation of a NamedObjectPersister for DataSet objects
  *
  */
class DataSetPersister[T] extends NamedObjectPersister[NamedDataSet[T]] {

  override def persist(namedObj: NamedDataSet[T], name: String) {
    namedObj match {
      case NamedDataSet(ds, forceComputation, storageLevel) =>
        require(!forceComputation || storageLevel != StorageLevel.NONE,
                "forceComputation implies storageLevel != NONE")
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        ds.persist(storageLevel)
        // perform some action to force computation
        if (forceComputation) ds.count()
    }
  }

  override def unpersist(namedObj: NamedDataSet[T]) {
    namedObj match {
      case NamedDataSet(ds, _, _) =>
        ds.unpersist(blocking = false)
    }
  }

  /**
    * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
    * garbage collected by Spark for some time.
    * @param namedDF the NamedDataFrame to refresh
    */
  override def refresh(namedDS: NamedDataSet[T]): NamedDataSet[T] =
    namedDS match {
      case NamedDataSet(ds, _, storageLevel) =>
        ds.persist(storageLevel)
        namedDS
    }

}
