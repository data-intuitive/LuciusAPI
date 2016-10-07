package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark.jobserver._

import scala.util.Try

/**
  * Common functionality, encapsulating the fact that we want to run tests outside of jobserver as well.
  */
object Common extends Serializable {

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  /**
    * Persist the database, depending on the situation:
    *
    * - As a NamedObject: When running inside Spark Jobserver
    * - As a named RDD in the Spark Context
    *
    */
  def persistDb(sc:SparkContext, sj:SparkJob with NamedObjectSupport with Globals, db:RDD[DbRow]) = {
    // Persist, depending on whether jobserver is running or not
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      sj.namedObjects.update("db", NamedRDD(db.cache, forceComputation = false, storageLevel = StorageLevel.NONE))
    } else {
      sj.setDb(db)
    }
  }

  /**
    * Persist the gene annotations, depending on the situation:
    *
    * - As a NamedObject: When running inside Spark Jobserver
    * - Using a global object (see `Global` trait)
    *
    */
  def persistGenes(sc:SparkContext, sj:SparkJob with NamedObjectSupport with Globals, genes:Broadcast[Genes]) = {
    // Persist, depending on whether jobserver is running or not
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      sj.namedObjects.update("genes", NamedBroadcast(genes))
    } else {
      sj.setGenes(genes)
    }
  }

  /**
    * Retrieve a pointer to the database, either using NamedObjects or via named RDD.
    */
  def retrieveDb(sc:SparkContext, sj:SparkJob with NamedObjectSupport with Globals):RDD[DbRow] = {
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      val NamedRDD(db, _ ,_) = sj.namedObjects.get[NamedRDD[DbRow]]("db").get
      db
    } else {
      initialize.getDb(sc)
    }
  }

  /**
    * Retrieve a pointer to the genes, either using NamedObjects or via `Global`.
    */
  def retrieveGenes(sc:SparkContext, sj:SparkJob with NamedObjectSupport with Globals):Broadcast[Genes] = {
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      val NamedBroadcast(genes) = sj.namedObjects.get[NamedBroadcast[Genes]]("genes").get
      genes
    } else {
      initialize.getGenes
    }
  }

}
