package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{regexp_replace, split}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.{FileWriter, PrintWriter}

object FileManager{

  /*
  * Save results to local file
  * */
  def writeResults(path:String, countingType:String, results:(List[RDD[(String, Int)]], Double)
                  ): Unit = {
    val pw = new PrintWriter(new FileWriter(path))

    val toPrint = if (countingType == "non-canonical") {
      List("\n> Non-canonical k-mers:").zip(results._1)
    }
    else {
      List("\n> Canonical k-mers:", "\n> Non-canonical k-mers:").zip(results._1)
    }

    pw.println("Execution time: " + results._2 + "sec.\n")
    toPrint.foreach(m => (pw.println(m._1), m._2.collect().foreach(k => (pw.print("(" + k._1 + ", "), pw.println(k._2 + ")")))))

    pw.close()
  }

  /*
  * Read FASTA file into RDD
  * (for sequential and parallel algorithms)
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