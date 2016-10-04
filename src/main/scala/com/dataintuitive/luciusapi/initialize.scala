package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.io.GenesIO
import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import spark.jobserver._

import scala.util.Try

object initialize extends SparkJob with NamedObjectSupport {

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val namespace = sc.getConf.get("spark.app.name")
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

    // Load data
    val db:RDD[DbRow] = sc.objectFile(location + "db")

    val jobServerRunning = Try(this.namedObjects).toOption
    if (jobServerRunning != None) {
      // This means we're really running within the jobserver, not within a notebook
//      this.namedRdds.update(namespace + "genes", genes.cache)
      namedObjects.update(namespace + "db", NamedRDD(db.cache, forceComputation = true, storageLevel = StorageLevel.NONE))
      namedObjects.getNames
    } else {
//      genes.cache.setName("genes")
      db.cache.setName("db")
      sc.getPersistentRDDs
    }



  }

}