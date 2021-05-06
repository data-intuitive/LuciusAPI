package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore._
import model.v4._
import genes._
import api._
import io.GenesIO._

import Common.ParamHandlers._
import com.dataintuitive.jobserver._
import Common._

import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver.SparkSessionJob
import spark.jobserver._

import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

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
      // case (parquet, "v1") => parquet.as[OldDbRow].map(_.toDbRow)
      // case (parquet, "v3") => parquet.as[DbRow]
      case (parquet, _)  => parquet.as[Perturbation]
    }
    val db = dbRaw.repartition(data.partitions)

    val dbNamedDataset = NamedDataSet[Perturbation](db, forceComputation = true, storageLevel = data.storageLevel)

    runtime.namedObjects.update("db", dbNamedDataset)

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

    val flatDbNamedDataset = NamedDataSet[FlatDbRow](flatDb, forceComputation = true, storageLevel = data.storageLevel)

    runtime.namedObjects.update("flatdb", flatDbNamedDataset)

    Map(
      "info" -> "Initialization done",
      "header" -> "None",
      "data" -> (db.rdd.count, flatDb.rdd.count, genes)
    )
  }

}

