package luciusapi

import LuciusBack.L1000._
import LuciusBack.RddFunctions._
import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd._
import spark.jobserver.{NamedRddSupport, SparkJob, SparkJobValid, SparkJobValidation}
import scala.util.Try

object initialize extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val namespace = sc.getConf.get("spark.app.name")
    val location:String = Try(config.getString("location")).getOrElse("hdfs://ly-1-09:54310/lucius1/")

    val fs_s3_awsAccessKeyId      = sys.env.get("AWS_ACCESS_KEY_ID").getOrElse("<MAKE SURE KEYS ARE EXPORTED>")
    val fs_s3_awsSecretAccessKey  = sys.env.get("AWS_SECRET_ACCESS_KEY").getOrElse("<THE SAME>")

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", fs_s3_awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", fs_s3_awsSecretAccessKey)

    val genes:RDD[LuciusBack.GeneFunctions.Gene] = sc.objectFile(location + "t_genes")    
    val translationTable:RDD[(String,String)] = sc.objectFile(location + "t_translationTable")
    val aRanks:RDD[LuciusBack.AnnotatedRanks] = sc.objectFile(location + "t_aRanks")
    val annotatedTPStats:RDD[LuciusBack.AnnotatedTPStats] = sc.objectFile(location + "t_aTPStats")

    val jobServerRunning = Try(this.namedRdds).toOption
    if (jobServerRunning != None) {
      // This means we're really running within the jobserver, not within a notebook
      this.namedRdds.update(namespace + "genes", genes.cache)
      this.namedRdds.update(namespace + "translationTable", translationTable.cache)
      this.namedRdds.update(namespace + "aRanks", aRanks.cache)
      this.namedRdds.update(namespace + "annotatedTPStats", annotatedTPStats.cache)
    } else {
      genes.cache.setName("genes")
      translationTable.cache.setName("translationTable")
      aRanks.cache.setName("aRanks")
      annotatedTPStats.cache.setName("annotatedTPStats")
    }

    (genes, translationTable, aRanks, annotatedTPStats)

  }

}