package com.dataintuitive.luciusapi

// Functions implementation and common code
import functions.StatisticsFunctions._

// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.GeneModel._

// Jobserver
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver._

// Scala, Scalactic and Typesafe
import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Common functionality, encapsulating the fact that we want to run tests outside of jobserver as well.
  */
object Common extends Serializable {

  // TODO: versions should be argument to versionMatch, no globals!
  val VERSIONS = Set("v1", "v2", "t1")

  implicit def rddPersister[T]: NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] =
    new BroadcastPersister[U]
  implicit def DataSetPersister[T]: NamedObjectPersister[NamedDataSet[T]] = new DataSetPersister[T]

  case class CachedData(db: Dataset[DbRow], genes: Genes)

  def paramSignature(config: Config): List[String] Or One[ValidationProblem] = {
    Try(config.getString("query").split(" ").toList)
      .map(q => Good(q))
      .getOrElse(Bad(One(SingleProblem("Signature query not provided"))))
  }

  def paramCompoundQ(config: Config): String Or One[ValidationProblem] = {
    Try(config.getString("query"))
      .map(q => Good(q))
      .getOrElse(Bad(One(SingleProblem("Parameter compound query not provided"))))
  }

  def paramTargetQ(config: Config): String Or One[ValidationProblem] = {
    Try(config.getString("query"))
      .map(q => Good(q))
      .getOrElse(Bad(One(SingleProblem("Parameter target query not provided"))))
  }

  def paramCompounds(config: Config): List[String] Or One[ValidationProblem] = {
    Try(config.getString("query").split(" ").toList)
      .map(q => Good(q))
      .getOrElse(Bad(One(SingleProblem("Parameter compounds not provided"))))
  }

  def paramSamples(config: Config): List[String] Or One[ValidationProblem] = {
    Try(config.getString("samples").split(" ").toList)
      .map(q => Good(q))
      .getOrElse(Bad(One(SingleProblem("Parameter samples not provided"))))
  }

  def paramSorted(config: Config): Boolean Or One[ValidationProblem] = {
    Try(config.getString("sorted").toBoolean)
      .map(q => Good(q))
      .getOrElse(Bad(One(SingleProblem("Parameter sorted not provided or not boolean"))))
  }

  def optParamBinsX(config: Config, default: Int = 20): Int = {
    Try(config.getString("binsX").toInt).getOrElse(default)
  }

  def optParamBins(config: Config, default: Int = 15): Int = {
    Try(config.getString("binsX").toInt).getOrElse(default)
  }

  def optParamBinsY(config: Config, default: Int = 20): Int = {
    Try(config.getString("binsY").toInt).getOrElse(default)
  }

  def optParamSignature(config: Config, default: List[String] = List(".*")): List[String] = {
    Try(config.getString("query").split(" ").toList).getOrElse(default)
  }

  def optParamVersion(config: Config, default: String = "v2"): String = {
    Try(config.getString("version")).getOrElse(default)
  }

  def optParamFilterConcentration(config: Config, default: String = ""): String = {
    Try(config.getString("filter.concentration")).getOrElse(default)
  }

  def optParamFilterProtocol(config: Config, default: String = ""): String = {
    Try(config.getString("filter.protocol")).getOrElse(default)
  }

  def optParamFilterType(config: Config, default: String = ""): String = {
    Try(config.getString("filter.type")).getOrElse(default)
  }

  def optParamFilters(config: Config): Map[String, String] = {
    Map(
      "concentration" -> optParamFilterConcentration(config),
      "type" -> optParamFilterType(config),
      "protocol" -> optParamFilterProtocol(config)
    )
  }

  def validVersion(config: Config): Boolean Or One[ValidationProblem] = {
    if (VERSIONS contains optParamVersion(config)) Good(true)
    else Bad(One(SingleProblem("Not a valid version identifier")))
  }

  def optParamPwids(config: Config, default: List[String] = List(".*")): List[String] = {
    Try(config.getString("pwids").split(" ").toList).getOrElse(default)
  }

  def optParamLimit(config: Config, default: Int = 10): Int = {
    Try(config.getString("limit").toInt).getOrElse(default)
  }

  def optParamFeatures(config: Config, default: List[String] = List(".*")): List[String] = {
    Try(config.getString("features").toString.split(" ").toList).getOrElse(default)
  }

  def getDB(runtime: JobEnvironment): Dataset[DbRow] Or One[ValidationProblem] = {
    Try {
      val NamedDataSet(db, _, _) = runtime.namedObjects.get[NamedDataSet[DbRow]]("db").get
      db
    }.map(db => Good(db))
      .getOrElse(Bad(One(SingleProblem("Cached DB not available"))))
  }

  def getGenes(runtime: JobEnvironment): Genes Or One[ValidationProblem] = {
    Try {
      val NamedBroadcast(genes) = runtime.namedObjects.get[NamedBroadcast[Genes]]("genes").get
      genes.value
    }.map(genes => Good(genes))
      .getOrElse(Bad(One(SingleProblem("Broadcast genes not available"))))
  }

  def paramDb(config: Config): String Or One[ValidationProblem] = {
    Try(config.getString("db"))
      .map(db => Good(db))
      .getOrElse(Bad(One(SingleProblem("DB config parameter not provided"))))
  }

  def paramGenes(config: Config): String Or One[ValidationProblem] = {
    Try(config.getString("geneAnnotations"))
      .map(ga => Good(ga))
      .getOrElse(Bad(One(SingleProblem("geneAnnotations config parameter not provided"))))
  }

  def paramPartitions(config: Config): Int = {
    // Optional parameter, default is 24
    Try(config.getString("partitions").toInt)
      .getOrElse(24)
  }

  def paramStorageLevel(config: Config): StorageLevel = {
    // Optional parameter, default is 24
    Try(config.getString("storageLevel")).toOption
      .flatMap(_ match {
        case "MEMORY_ONLY"   => Some(StorageLevel.MEMORY_ONLY)
        case "MEMORY_ONLY_2" => Some(StorageLevel.MEMORY_ONLY_2)
        case _               => None
      })
      .getOrElse(StorageLevel.MEMORY_ONLY)
  }

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
  def persistDb(sc: SparkContext,
                sj: SparkJob with NamedObjectSupport with Globals,
                db: RDD[DbRow]) = {
    // Persist, depending on whether jobserver is running or not
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      sj.namedObjects.update(
        "db",
        NamedRDD(db.cache, forceComputation = false, storageLevel = StorageLevel.NONE))
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
  def persistGenes(sc: SparkContext,
                   sj: SparkJob with NamedObjectSupport with Globals,
                   genes: Broadcast[Genes]) = {
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
  def retrieveDb(sc: SparkContext, sj: SparkJob with NamedObjectSupport with Globals): RDD[DbRow] = {
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      val NamedRDD(db, _, _) = sj.namedObjects.get[NamedRDD[DbRow]]("db").get
      db
    } else {
      sj.getDb(sc)
    }
  }

  /**
    * Retrieve a pointer to the genes, either using NamedObjects or via `Globals`.
    */
  def retrieveGenes(sc: SparkContext,
                    sj: SparkJob with NamedObjectSupport with Globals): Broadcast[Genes] = {
    val jobServerRunning = Try(sj.namedObjects).toOption
    if (jobServerRunning.isDefined) {
      // This means we're really running within the jobserver, not within a notebook
      val NamedBroadcast(genes) = sj.namedObjects.get[NamedBroadcast[Genes]]("genes").get
      genes
    } else {
      initialize.getGenes
    }
  }

  def runTests(tests: Seq[(String, (Option[String] => Boolean, String))], config: Config) = {
    val configsExtracted = tests.map(c => (Try(config.getString(c._1)).toOption, c._2))
    val t = configsExtracted.map { case (value, (func, error)) => (func(value), error) }

    t.reduce((a, b) =>
      (a, b) match {
        case ((true, l), (true, r))   => (true, "")
        case ((true, l), (false, r))  => (false, r)
        case ((false, l), (true, r))  => (false, l)
        case ((false, l), (false, r)) => (false, l + ", " + r)
    })
  }

  /**
    * Function that takes a `Seq` of `SingleConfigTest` and performs the tests one by one.
    */
  def runSingleParValidations(tests: SingleParValidations, config: Config): Seq[(Boolean, String)] = {
    val configsExtracted = tests.map(c => (Try(config.getString(c._1)).toOption, c._2))
    configsExtracted.map { case (value, (func, error)) => (func(value), error) }
  }

  /**
    * Function that takes a `Seq` of `CombinedConfigTest` and performs the tests one by one.
    */
  def runCombinedParValidations(tests: CombinedParValidations,
                                config: Config): Seq[(Boolean, String)] = {
    val configsExtracted = tests.map {
      case (seqParams, tuple) => (seqParams.map(x => Try(config.getString(x)).toOption), tuple)
    }
    configsExtracted.map { case (value, (func, error)) => (func(value), error) }
  }

  /**
    * Aggregate results of single and combined validations.
    */
  def aggregateValidations(tests: Seq[(Boolean, String)]): (Boolean, String) = {
    tests.reduce((a, b) =>
      (a, b) match {
        case ((true, l), (true, r))   => (true, "")
        case ((true, l), (false, r))  => (false, r)
        case ((false, l), (true, r))  => (false, l)
        case ((false, l), (false, r)) => (false, l + ", " + r)
    })
  }

  def isDefined(x: Option[String]): Boolean = x.isDefined
  def isNotEmpty(x: Option[String]): Boolean = x.exists(_.trim != "")

  def versionMatch(x: Option[String]): Boolean = x.exists(v => VERSIONS contains v)

}
