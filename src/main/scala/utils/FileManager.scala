package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object FileManager{

  /*
  * Save results to local file
  * */
  def writeResults(path:String, countingType:String, results:(List[Array[(String, Int)]], Double), sparkContext:SparkContext
                  ): Unit = {

    val withHeaders = sparkContext.parallelize(if (countingType == "non-canonical") {
      List("\n> Non-canonical k-mers:").zip(results._1)
    }
    else {
      List("\n> Canonical k-mers:", "\n> Non-canonical k-mers:").zip(results._1)
    })
    val res = withHeaders.map(m => m._1 + m._2.map(k => ("(" + k._1 + ", " + k._2.toString + ")")).mkString("", "\n", "")+"\n")
    res.coalesce(1, shuffle = true).saveAsTextFile(path)
  }


  /*
  * Read FASTA file into RDD
  * (for sequential and distributed algorithms)
  * */
  def readFASTAtoRDD(fileName: String, sparkContext: SparkContext):RDD[String]  = {
    //loading the fasta file
    val genSeq = sparkContext.textFile(fileName)

    //removing comment lines (but keeping headers ">")
    genSeq.filter( line => !line.startsWith(";") )
  }


  /*
  * Read FASTA file into dataframe
  * (for library algorithm)
  * */
  def readFASTAtoDF(fileName: String, sparkSession: SparkSession):Dataset[Row]  = {
    import sparkSession.implicits._

    // read genomic sequence (in "sequence reads")
    val df = sparkSession.read.option("lineSep", ">").textFile(fileName)

    //remove header lines
    val genSeq = df.withColumn("value", regexp_replace($"value", "^.*\\n", ""))
      .withColumn("value", regexp_replace($"value", "^;.+\\n", "")).filter(r => r.mkString.nonEmpty)

    genSeq
  }

}