package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark.jobserver._
import com.typesafe.config.Config

import scala.util.Try

/**
  * Common functionality, encapsulating the fact that we want to run tests outside of jobserver as well.
  */
object Common extends Serializable {

  // TODO: versions should be argument to versionMatch, no globals!
  val VERSIONS = Set("v1", "v2", "t1")

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  type SinglePar = String
  type CombinedPar = Seq[String]
  type SingleParValidator = Option[String] => Boolean
  type CombinedParValidator = Seq[Option[String]] => Boolean
  type SingleParValidation = (SinglePar, (SingleParValidator, String))
  type CombinedParValidation = (CombinedPar, (CombinedParValidator, String))
  type SingleParValidations = Seq[SingleParValidation]
  type CombinedParValidations = Seq[CombinedParValidation]

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
      sj.namedObjects.update("db", NamedRDD(db.cache, forceComputation = true, storageLevel = StorageLevel.NONE))
    } else {
      sj.setDb(db)
    }
  }

  /**
    * Persist the gene annotations, depending on the situation:
    *
    * - As a NamedObject: When running inside Spark Jobserver
    * - Using a global object (see `Globals` trait)
    *
    */
  def persistGenes(sc:SparkContext, sj:SparkJob with NamedObjectSupport with Globals, genes:Broadcast[Genes]) = {
    // Persist, depending on whether jobserver is running or not
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      sj.namedObjects.update("genes", NamedBroadcast(genes))
    } else {
      initialize.setGenes(genes)
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
      sj.getDb(sc)
    }
  }

  /**
    * Retrieve a pointer to the genes, either using NamedObjects or via `Globals`.
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

  def runTests(tests:Seq[(String, (Option[String] => Boolean, String))], config:Config) = {
    val configsExtracted = tests.map(c => (Try(config.getString(c._1)).toOption, c._2))
    val t = configsExtracted.map { case (value, (func, error)) => (func(value), error) }

    t.reduce((a, b) => (a, b) match {
      case ((true, l), (true, r)) => (true, "")
      case ((true, l), (false, r)) => (false, r)
      case ((false, l), (true, r)) => (false, l)
      case ((false, l), (false, r)) => (false, l + ", " + r)
    })
  }

  /**
    * Function that takes a `Seq` of `SingleConfigTest` and performs the tests one by one.
    */
  def runSingleParValidations(tests:SingleParValidations, config:Config):Seq[(Boolean, String)] = {
    val configsExtracted = tests.map(c => (Try(config.getString(c._1)).toOption, c._2))
    configsExtracted.map { case (value, (func, error)) => (func(value), error) }
  }

  /**
    * Function that takes a `Seq` of `CombinedConfigTest` and performs the tests one by one.
    */
  def runCombinedParValidations(tests:CombinedParValidations, config:Config):Seq[(Boolean, String)] = {
    val configsExtracted = tests.map{ case (seqParams, tuple) => (seqParams.map( x => Try(config.getString(x)).toOption), tuple) }
    configsExtracted.map { case (value, (func, error)) => (func(value), error) }
  }

  /**
    * Aggregate results of single and combined validations.
    */
  def aggregateValidations(tests:Seq[(Boolean, String)]):(Boolean, String) = {
    tests.reduce((a, b) => (a, b) match {
      case ((true, l), (true, r)) => (true, "")
      case ((true, l), (false, r)) => (false, r)
      case ((false, l), (true, r)) => (false, l)
      case ((false, l), (false, r)) => (false, l + ", " + r)
    })
  }

  def isDefined(x: Option[String]):Boolean = x.isDefined
  def isNotEmpty(x: Option[String]):Boolean = x.exists(_.trim != "")

  def versionMatch(x: Option[String]): Boolean = x.exists(v => VERSIONS contains v)

}
