package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore._
import model.v4_1._
import genes._
import api.v4_1._
import io.GenesIO._
import io.State
import lenses.CombinedPerturbationLenses.safeCellLens

import Common.ParamHandlers._
import com.dataintuitive.jobserver._
import Common._

import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver.SparkSessionJob
import spark.jobserver._

import scala.util.control.Exception._
import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

object initialize extends SparkSessionJob with NamedObjectSupport {

  case class JobData(dbs: List[String],
                     geneAnnotations: String,
                     dbVersion: String,
                     partitions: Int,
                     storageLevel: StorageLevel)
  type JobOutput = collection.Map[String, Any]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = paramDbOrDbs(config)
    val genes = paramGenes(config)
    val dbVersion = paramDbVersion(config)
    val partitions = paramPartitions(config)
    val storageLevel = paramStorageLevel(config)

    withGood(db, genes) { JobData(_, _, dbVersion, partitions, storageLevel) }

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
    val genesDB = IO.getGenesDB(sparkSession, data.geneAnnotations)
    val genesBC = sparkSession.sparkContext.broadcast(genesDB)

    runtime.namedObjects.update("genes", NamedBroadcast(genesBC))

    val outputs = IO.allInput(sparkSession, data.dbs)
    val state = State(outputs)

    val thisVersion = state.state.filter(_.version.major.toString == data.dbVersion)

    println(outputs)
    println(state)
    println(thisVersion)

    val parquets = thisVersion.map(_.obj.toString).map(
      sparkSession.read
        .schema(Encoders.product[Perturbation].schema) // This assists parquet file reading so that it is more independent of our current Perturbation format.
                                                       // Without adding the schema, the parquet needs to be 100% similar to Perturbations.
                                                       // At the moment of writing, this was needed because the parquet files only have 4 treatment types but the
                                                       // Perturbation class have the full 14 types.
        .parquet(_)
    )
    val dbRaws = parquets.map{ parquet =>
      (parquet, data.dbVersion) match {
        case (parquet, _)  => parquet.as[Perturbation]
      }
    }
    val db = dbRaws.reduce(_ union _).repartition(data.partitions)

    val dbNamedDataset = NamedDataSet[Perturbation](db, forceComputation = true, storageLevel = data.storageLevel)

    runtime.namedObjects.update("db", dbNamedDataset)

    val flatDb = db.map( row =>
          FlatDbRow(
            row.id,
            safeCellLens.get(row),
            row.trt.trt_cp.map(_.dose).getOrElse("N/A"),
            row.trtType,
            row.trt.trt.name,
            row.profiles.profile.map(_.p.map(_.count(_ <= 0.05)).getOrElse(0) > 0).getOrElse(false)
          )
        )

    val flatDbNamedDataset = NamedDataSet[FlatDbRow](flatDb, forceComputation = true, storageLevel = data.storageLevel)

    runtime.namedObjects.update("flatdb", flatDbNamedDataset)

    // Cache filters
    val filters = Filters.calculate(db)(sparkSession)
    val filtersBC = sparkSession.sparkContext.broadcast(filters)

    runtime.namedObjects.update("filters", NamedBroadcast(filtersBC))

    Map(
      "info" -> "Initialization done",
      "header" -> "None",
      "data" -> (db.rdd.count, flatDb.rdd.count)
    )
  }

}

