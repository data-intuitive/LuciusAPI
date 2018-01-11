package com.dataintuitive.luciusapi

// Functions implementation and common code
import Common._

// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.GeneModel._
import com.dataintuitive.luciuscore.TransformationFunctions
import com.dataintuitive.luciuscore.SignatureModel._

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

object generateSignature extends SparkSessionJob with NamedObjectSupport {

  case class JobData(db: Dataset[DbRow], genes: Genes, version: String, samples: List[String])

  type JobOutput = Array[String]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)
    val samples = paramSamples(config)

    (isValidVersion zip
      withGood(db, genes, samples) {
        JobData(_, _, version, _)
      }).map(_._2)

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    val JobData(db, genes, version, samples) = data

    // Start with query compound and retrieve indices
    val selection =
      db.filter(x => x.pwid.exists(elem => samples.toSet.contains(elem)))
        .collect
        .map(x => (x.sampleAnnotations.t.get, x.sampleAnnotations.p.get))
    val valueVector = TransformationFunctions.aggregateStats(selection, 0.05)
    val rankVector = TransformationFunctions.stats2RankVector((valueVector, Array()))

    val indexSignature: IndexSignature =
      TransformationFunctions.rankVector2IndexSignature(rankVector)

    // dict's
    val symbolDict = genes.symbol2ProbesetidDict
    val indexDict = genes.index2ProbesetidDict

    val probesetidSignature = indexSignature.translate2Probesetid(indexDict)
    val symbolSignature = probesetidSignature.translate2Symbol(symbolDict)

    symbolSignature.signature

  }

}
