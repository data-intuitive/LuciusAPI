package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.io._

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver.{NamedRddSupport, SparkJob, SparkJobValid, SparkJobValidation}
import scala.util.Try

/**
  * This object/endpoint transforms the input files for Lucius into an intermediate representation
  * for fast initialization and query.
  *
  * Please see `io` tests in LuciusCore for the procedure.
  */
object preprocess extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

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

    // Loading the data
    val dbRelations = SampleCompoundRelationsIO.loadSampleCompoundRelationsFromFileV1(sc, sampleCompoundRelationsFile)
    val compoundAnnotations = CompoundAnnotationsIO.loadCompoundAnnotationsFromFileV2(sc, compoundAnnotationsFile)
    val dbAfterCA = CompoundAnnotationsIO.updateCompoundAnnotationsV2(compoundAnnotations, dbRelations)
    val tStats = StatsIO.loadStatsFromFile(sc, tStatsFile, toTranspose=true)
    val pStats = StatsIO.loadStatsFromFile(sc, pStatsFile, toTranspose=true)
    val dbAfterStatsTmp = StatsIO.updateStats(tStats, dbAfterCA, StatsIO.dbUpdateT)
    val dbAfterStats = StatsIO.updateStats(pStats, dbAfterStatsTmp, StatsIO.dbUpdateP)
    val db = RanksIO.updateRanks(dbAfterStats)

    val genes = GenesIO.loadGenesFromFile(sc, geneAnnotationsFile)

    db.saveAsObjectFile(locationTo + "db")
    sc.parallelize(genes.genes).saveAsObjectFile(locationTo + "genes")

    (genes, db)

  }

}