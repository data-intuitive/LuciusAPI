package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.io._

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobInvalid, SparkJobValidation}
import scala.util.Try

/**
  * This object/endpoint transforms the input files for Lucius into an intermediate representation
  * for fast initialization and query.
  *
  * Please see `io` tests in LuciusCore for the procedure.
  */
object preprocess extends SparkJob {

  /**
    * Make sure:
    *   - `locationFrom` and `locationTo` are provided
    *   - Todo: Check if Amazon keys are exported (when using S3 locations)
    *   - Todo: Both locations exist
    */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val locationFromOption = Try(config.getString("locationFrom")).toOption
    val locationToOption = Try(config.getString("locationTo")).toOption

    val fsS3AwsAccessKeyId      = sys.env.get("AWS_ACCESS_KEY_ID")
    val fsS3AwsSecretAccessKey  = sys.env.get("AWS_SECRET_ACCESS_KEY")

    val tests = Seq(
      (locationFromOption.isDefined, "locationFrom not defined in POST config"),
      (locationToOption.isDefined, "locationTo not defined in POST config"),
      (fsS3AwsAccessKeyId.isDefined, "AWS Access key not exported in shell"),
      (fsS3AwsSecretAccessKey.isDefined, "AWS Secret key not exported in shell")
    )

    val allTests = tests.reduce((a,b) => (a,b) match{
      case ((true, l), (true, r))  => (true, "")
      case ((true, l), (false, r)) => (false, r)
      case ((false, l), (true, r)) => (false, l)
      case ((false, l), (false, r)) => (false, l + ", " + r)
    })

    if (allTests._1) SparkJobValid
    else SparkJobInvalid(allTests._2)

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // API Version
    val version = Try(config.getString("version")).getOrElse("v1")

    val namespace = sc.getConf.get("spark.app.name")
    val locationFrom:String = Try(config.getString("locationFrom")).getOrElse("s3n://itx-abt-jnj-exasci/L1000/")
    val locationTo:String = Try(config.getString("locationTo")).getOrElse("hdfs://ly-1-09:54310/lucius1/")

    // S3 keys
    val fs_s3_awsAccessKeyId      = sys.env.get("AWS_ACCESS_KEY_ID").getOrElse("<MAKE SURE KEYS ARE EXPORTED>")
    val fs_s3_awsSecretAccessKey  = sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("<THE SAME>")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", fs_s3_awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", fs_s3_awsSecretAccessKey)

    // Input file locations based on config
    val base = locationFrom
    val sampleCompoundRelationsFile = base + "phenoData.txt"
    val pStatsFile = base + "tPvalues.txt"
    val tStatsFile = base + "tStats.txt"
    val geneAnnotationsFile = base + "featureData.txt"
    val compoundAnnotationsFile = base + "compoundAnnotations.tsv"

    val db = version match {
      case "v1" => {
        // Loading the data (v1 format) -- All compound info is in the relations file
        val dbRelations = SampleCompoundRelationsIO.loadSampleCompoundRelationsFromFileV1(sc, sampleCompoundRelationsFile)
        val tStats = StatsIO.loadStatsFromFile(sc, tStatsFile, toTranspose = true)
        val pStats = StatsIO.loadStatsFromFile(sc, pStatsFile, toTranspose = true)
        val dbAfterStatsTmp = StatsIO.updateStats(tStats, dbRelations, StatsIO.dbUpdateT)
        val dbAfterStats = StatsIO.updateStats(pStats, dbAfterStatsTmp, StatsIO.dbUpdateP)
        RanksIO.updateRanks(dbAfterStats)
      }
      case "v2" => {
        // Loading the data (v2 format) -- Compound info is in annotations file
        val dbRelations = SampleCompoundRelationsIO.loadSampleCompoundRelationsFromFileV1(sc, sampleCompoundRelationsFile)
        val compoundAnnotations = CompoundAnnotationsIO.loadCompoundAnnotationsFromFileV2(sc, compoundAnnotationsFile)
        val dbAfterCA = CompoundAnnotationsIO.updateCompoundAnnotationsV2(compoundAnnotations, dbRelations)
        val tStats = StatsIO.loadStatsFromFile(sc, tStatsFile, toTranspose = true)
        val pStats = StatsIO.loadStatsFromFile(sc, pStatsFile, toTranspose = true)
        val dbAfterStatsTmp = StatsIO.updateStats(tStats, dbAfterCA, StatsIO.dbUpdateT)
        val dbAfterStats = StatsIO.updateStats(pStats, dbAfterStatsTmp, StatsIO.dbUpdateP)
        RanksIO.updateRanks(dbAfterStats)
      }
    }

    // Loading gene annotations
    val genes = GenesIO.loadGenesFromFile(sc, geneAnnotationsFile)

    // Writing to intermediate object format
    db.saveAsObjectFile(locationTo + "db")
    sc.parallelize(genes.genes).saveAsObjectFile(locationTo + "genes")

    // Output something to check
    (genes.genes.head, db.first)

  }

}