package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.NGram
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import utils.FileManager
import utils.GenomicUtils.{reverseComplement, transformBases}

class NGramCounting(fileName: String, sparkContext: SparkContext, sparkSession: SparkSession,
                    k:Broadcast[Int]) extends CountingAlgorithm(fileName, sparkContext, k)  with CountingType {
  import sparkSession.implicits._

  override type T = DataFrame
  override type S = Dataset[Row]

  //read the FASTA file
  override val sequence: S = FileManager.readFASTAtoDF(fileName,sparkSession)

  override def _kmerExtraction(k: Broadcast[Int]): T = {

    val genSeq = sequence.map(r => transformBases(r.mkString).split(""))

    val ngrammer = new NGram().setN(k.value).setInputCol("value").setOutputCol("ngrams")
    val libKmers = ngrammer.transform(genSeq)

    libKmers
  }


  override def _counting(kmers: T, canonical: Boolean): RDD[(String, Int)] = {

    val allKmers = kmers.select(explode(col("ngrams")).alias("ngrams"))
    val filteredKmers = allKmers.map(r => r.mkString.replace(" ", "")).filter(kmer => !kmer.contains("N"))

    val kmerGroupped = {
      if (canonical){
        filteredKmers.map(kmer => reverseComplement(kmer)).groupBy("value").count().rdd
      }
      else {
        filteredKmers.groupBy("value").count().rdd
      }
    }

    //TODO rimuovi stampa
    kmerGroupped.map(r => (r(0).toString, r(1).toString.toInt)).foreach(println)

    kmerGroupped.map(r => (r(0).toString, r(1).toString.toInt))

    //    val printa = mahhh.map(r => r.mkString("(", ",", ")"))
  }

}
