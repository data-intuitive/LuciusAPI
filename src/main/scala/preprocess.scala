package luciusapi

import LuciusBack.L1000._
import LuciusBack.RddFunctions._
import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver.{NamedRddSupport, SparkJob, SparkJobValid, SparkJobValidation}
import scala.util.Try

object preprocess extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val namespace = sc.getConf.get("spark.app.name")
    val locationFrom:String = Try(config.getString("locationFrom")).getOrElse("s3n://itx-abt-jnj-exasci/L1000/")
    val locationTo:String = Try(config.getString("locationTo")).getOrElse("hdfs://ly-1-09:54310/lucius1/")

    val fs_s3_awsAccessKeyId      = sys.env.get("AWS_ACCESS_KEY_ID").getOrElse("<MAKE SURE KEYS ARE EXPORTED>")
    val fs_s3_awsSecretAccessKey  = sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("<THE SAME>")

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", fs_s3_awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", fs_s3_awsSecretAccessKey)

    val tStatsFile = locationFrom + "tStats.txt"
    val pStatsFile = locationFrom + "tPvalues.txt"
    val phenoFile = locationFrom + "phenoData.txt"
    val tStats = loadGenesPwidsValues(sc, tStatsFile, "\t", true)
    val pStats = loadGenesPwidsValues(sc, pStatsFile, "\t", true)
    val pheno = loadFeaturesPwidsAnnotations(sc, phenoFile, "\t")
    val translationTable = loadGeneTranslationTable(sc, locationFrom + "featureData.txt", "\t").cache

    // Calculate ranks and add annotations
    val ranks = tStats.data.map(valueVector2AvgRankVector(_))
    val aRanks = joinPwidsRanksAnnotations(tStats.pwids, ranks, pheno.annotations)

    // Derived signatures from pwids, based on significance
    val tpStats = joinAndZip(tStats.data, pStats.data)
    val signaturesFromPwids = tpStats.map(x => generateSignature(x,tStats.genes,0.05,100))

    // Derived signatures from multiple pwids, pre-populate RDD for later derivation
    val annotatedTPStats = joinPwidsTPAnnotations(tStats.pwids, tStats.data, pStats.data, pheno.annotations)

    val genes = sc.parallelize(tStats.genes)

    genes.saveAsObjectFile(locationTo + "t_genes")
    translationTable.saveAsObjectFile(locationTo + "t_translationTable")
    aRanks.saveAsObjectFile(locationTo + "t_aRanks")
    annotatedTPStats.saveAsObjectFile(locationTo + "t_aTPStats")


    (genes, translationTable, aRanks, annotatedTPStats)

  }

}