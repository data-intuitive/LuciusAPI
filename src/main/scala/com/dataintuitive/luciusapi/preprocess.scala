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

  def isDefined(x: Option[String]):Boolean = x.isDefined

  // TODO: versions should be argument to versionMatch, no globals!
  val versions = Set("v1", "v2", "t1")
  def versionMatch(x: Option[String]): Boolean = x.exists(v => versions contains v)

//  def isCompoundAnnotationsV2(tuple: (Option[String], Option[String])): Boolean = tuple match {
//    case (ca, v) => (v.getOrElse("vv") == "v2") && ca.isDefined
//  }

  // This is ok if no combination of parameters are required
  val configs:Seq[(String, (Option[String] => Boolean, String))] = Seq(
    ("locationFrom",           (isDefined ,    "locationFrom not defined in POST config")),
    ("locationTo",             (isDefined ,    "locationTo not defined in POST config")  ),
    ("sampleCompoundRelations",(isDefined ,    "sampleCompoundRelations not defined in POST config")  ),
    ("geneAnnotations",        (isDefined ,    "geneAnnotations not defined in POST config")  ),
    ("tStats",                 (isDefined ,    "tStats not defined in POST config")  ),
    ("pStats",                 (isDefined ,    "pStats not defined in POST config")  ),
    ("version",                (isDefined ,    "version not defined in POST config")),
    ("version",                (versionMatch , "version should be either v1, v2 or t1"))
  )

//  val configsCorr = Map(
//    (("version", "compoundAnnotations"), (isCompoundAnnotationsV2 _, "compoundAnnotations required for v2")),
//  )


  /**
    * Make sure:
    *   - `locationFrom` and `locationTo` are provided
    *   - Todo: Both locations exist
    *   - Todo: Check if target already exists
    */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val configsExtracted = configs.map(c => (Try(config.getString(c._1)).toOption, c._2))
    val testsRun = configsExtracted.map{case (value, (func, error)) => (func(value), error)}

    println(configsExtracted.map(_._2._2))
    println(testsRun.map(_._2))

    val allTests = testsRun.reduce((a,b) => (a,b) match{
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
    val locationFrom:String = Try(config.getString("locationFrom")).get
    val locationTo:String = Try(config.getString("locationTo")).get
    val sampleCompoundRelationsString:String = Try(config.getString("sampleCompoundRelations")).get
    val geneAnnotationsString:String = Try(config.getString("geneAnnotations")).get
    val tStatsString:String = Try(config.getString("tStats")).get
    val pStatsString:String = Try(config.getString("pStats")).get

    // S3 keys, for backward compatibility
    val fs_s3_awsAccessKeyId      = sys.env.get("AWS_ACCESS_KEY_ID").getOrElse("<MAKE SURE KEYS ARE EXPORTED>")
    val fs_s3_awsSecretAccessKey  = sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("<THE SAME>")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", fs_s3_awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", fs_s3_awsSecretAccessKey)

    // Input file locations based on config
    val base = locationFrom
    val sampleCompoundRelationsFile = base + sampleCompoundRelationsString
    val pStatsFile = base + pStatsString
    val tStatsFile = base + tStatsString
    val geneAnnotationsFile = base + geneAnnotationsString

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
        val compoundAnnotationsString:String = Try(config.getString("compoundAnnotations")).get
        val compoundAnnotationsFile = base + compoundAnnotationsString

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
      case "t1" => {
        // Loading the data (v1 format) for the testing and demonstration dataset
        val dbRelations = SampleCompoundRelationsIO.loadSampleCompoundRelationsFromFileV1(sc, sampleCompoundRelationsFile)
        val tStats = StatsIO.loadStatsFromFile(sc, tStatsFile, toTranspose = false)
        val pStats = StatsIO.loadStatsFromFile(sc, pStatsFile, toTranspose = false)
        val dbAfterStatsTmp = StatsIO.updateStats(tStats, dbRelations, StatsIO.dbUpdateT)
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