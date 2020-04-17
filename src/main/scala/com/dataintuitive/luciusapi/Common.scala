package com.dataintuitive.luciusapi

// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.genes._

// Jobserver
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver._

// Scala, Scalactic and Typesafe
import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config
import collection.JavaConverters._

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.dataintuitive.luciusapi.Model.FlatDbRow

import com.dataintuitive.jobserver.NamedDataSet
import com.dataintuitive.jobserver.DataSetPersister

/**
  * Common functionality, encapsulating the fact that we want to run tests outside of jobserver as well.
  */
object Common extends Serializable {

  val VERSIONS = Set("v1", "v2", "t1")

  implicit def rddPersister[T]: NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] =
    new BroadcastPersister[U]
  implicit def DataSetPersister[T]: NamedObjectPersister[NamedDataSet[T]] = new DataSetPersister[T]

  case class CachedData(db: Dataset[DbRow], flatDb: Dataset[FlatDbRow], genesDB: GenesDB)

  object ParamHandlers {

    def paramSignature(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Signature query not provided"))))
    }

    def paramSignature1(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query1").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Signature query not provided"))))
    }

    def paramSignature2(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query2").split(" ").toList)
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

    def paramTargets(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter targets query not provided"))))
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

    def optParamHead(config: Config, default: Int = 0): Int = {
      Try(config.getString("head").toInt).getOrElse(default)
    }

    def optParamTail(config: Config, default: Int = 0): Int = {
      Try(config.getString("tail").toInt).getOrElse(default)
    }

    def validHeadTail(config: Config): Boolean Or One[ValidationProblem] = {
      if (optParamHead(config) > 0 || optParamTail(config) > 0) Good(true)
      else Bad(One(SingleProblem("Either head or tail count needs to be provided")))
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

    def optParamFilterConcentration(config: Config, default: List[String] = List()): List[String] = {
      Try(config.getStringList("filter.concentration")).map(_.asScala.toList).getOrElse(default)
    }

    def optParamFilterProtocol(config: Config, default: List[String] = List()): List[String] = {
      Try(config.getStringList("filter.protocol")).map(_.asScala.toList).getOrElse(default)
    }

    def optParamFilterType(config: Config, default: List[String] = List()): List[String] = {
      Try(config.getStringList("filter.type")).map(_.asScala.toList).getOrElse(default)
    }

    def optParamFilters(config: Config): Map[String, List[String]] = {
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

    def getFlatDB(runtime: JobEnvironment): Dataset[FlatDbRow] Or One[ValidationProblem] = {
      Try {
        val NamedDataSet(flatdb, _, _) = runtime.namedObjects.get[NamedDataSet[FlatDbRow]]("flatdb").get
        flatdb
      }.map(flatdb => Good(flatdb))
        .getOrElse(Bad(One(SingleProblem("Cached FlatDB not available"))))
    }

    def getGenes(runtime: JobEnvironment): GenesDB Or One[ValidationProblem] = {
      Try {
        val NamedBroadcast(genes) = runtime.namedObjects.get[NamedBroadcast[GenesDB]]("genes").get
        genes.value
      }.map(genes => Good(genes))
        .getOrElse(Bad(One(SingleProblem("Broadcast genes not available"))))
    }

    def paramDb(config: Config): String Or One[ValidationProblem] = {
      Try(config.getString("db.uri"))
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

    def paramDbVersion(config: Config, default: String = "v2"): String = {
      Try(config.getString("db.version")).getOrElse(default)
    }

    val defaultDict = Map(
        "probesetID" -> "probesetid",
        "dataType" -> "dataType",
        "ENTREZID" -> "entrezid",
        "ENSEMBL" -> "ensemblid",
        "SYMBOL" -> "symbol",
        "GENENAME" -> "name",
        "GENEFAMILY" -> "geneFamily"
        )

    /**
     * geneFeatures contains the mapping between the input gene annotations file
     * and the features in the model.
     * This function parses this part of the config and falls back to the above if
     * `geneFeatures` is not present in the config.
     */
    def paramGeneFeatures(config: Config):Map[String, String] = {
      Try(config.getObject("geneFeatures")).toOption
          .map(_.unwrapped.asScala.toMap.map{case (k,v) => (k.toString, v.toString)})
          .getOrElse(defaultDict)
    }

  }

  object Variables {

    // Calculated
    val ZHANG = Set("zhang", "similarity", "Zhang", "Similarity")

    // Sample
    val ID = Set("id", "pwid")
    val BATCH = Set("batch", "Batch")
    val PLATEID = Set("plateid", "PlateId")
    val WELL = Set("well", "Well")
    val PROTOCOLNAME = Set("protocolname", "cellline", "CellLine", "ProtocolName")
    val CONCENTRATION = Set("concentration", "Concentration")
    val YEAR = Set("year", "Year")
    val TIME = Set("time", "Time")

    // Compound
    val COMPOUND_ID = Set("jnjs", "Jnjs", "cid", "pid", "compound_id")
    val JNJB = Set("jnjb", "Jnjb")
    val COMPOUND_SMILES = Set("Smiles", "smiles", "SMILES", "compound_smiles")
    val COMPOUND_INCHIKEY = Set("inchikey", "Inchikey", "compound_inchikey")
    val COMPOUND_NAME = Set("compoundname", "CompoundName", "Compoundname", "name", "Name", "compound_name")
    val COMPOUND_TYPE = Set("Type", "type", "compound_type")
    val COMPOUND_TARGETS = Set("targets", "knownTargets", "Targets", "compound_targets")

    // Derived
    val SIGNIFICANTGENES = Set("significantGenes")

  }

}
