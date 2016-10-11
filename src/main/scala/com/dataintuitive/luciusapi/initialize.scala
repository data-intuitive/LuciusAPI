package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.GeneModel._
import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.io.GenesIO
import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import spark.jobserver._
import spark.jobserver.NamedBroadcast
import spark.jobserver.BroadcastPersister

import scala.util.Try

/**
  * Initialize the API by caching the database with sample-compound information
  *
  * We make a distinction between running within SparkJobserver and not.
  * The difference is in the handling of named objects in-between api calls.
  *
  * - For Jobserver we use NamedObject support for both the RDD and the dictionary of genes.
  * - For local, we use PersistentRDDs in combination with a new loading of the genes database at every call.
  */
object initialize extends SparkJob with NamedObjectSupport with Globals {

  import Common._

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Config
    val location:String = Try(config.getString("location")).getOrElse("")
    val base = location
    val geneAnnotationsString:String = Try(config.getString("geneAnnotations")).get

    // Backward compatibility
    val fs_s3_awsAccessKeyId      = sys.env.get("AWS_ACCESS_KEY_ID").getOrElse("<MAKE SURE KEYS ARE EXPORTED>")
    val fs_s3_awsSecretAccessKey  = sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("<THE SAME>")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", fs_s3_awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", fs_s3_awsSecretAccessKey)

    // Loading gene annotations
    val geneAnnotationsFile = base + geneAnnotationsString
    val genes = GenesIO.loadGenesFromFile(sc, geneAnnotationsFile)
    val broadcast = sc.broadcast(genes)

    // Load data
    val db:RDD[DbRow] = sc.objectFile(location + "db")

    persistDb(sc, this, db)
    persistGenes(sc, this, broadcast)

    "LuciusAPI initialized..."

  }

}