package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.GenomicUtils._

object SeqKmerCounting extends CountingAlgorithm {
  override type T = Array[(String,Int)]

  override def kmerExtraction(sequence: RDD[String], k:Broadcast[Int]): T = {
    val seq = transformBases(sequence.collect().mkString)

    //split the FASTA file into entries (genomic subsequence)
    val entries: Array[String] = seq.split(">")
    //extracting kmers
    entries.flatMap(_.sliding(k.value, 1).filter(kmer => !kmer.contains("N")).map((_, 1)))

  }

  override def counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = {

      val kmersGroupped: Map[String, Int] =
        if (canonical) {
          kmers.groupBy(_._1).map { case (k, v) => k -> v.map {_._2}.sum }
        }
        else {
          kmers.groupBy(kmer => reverseComplement(kmer._1)).map { case (k, v) => k -> v.map {_._2}.sum }
        }

      sparkContext.parallelize(kmersGroupped.toSeq)
  }

}


class SeqKmerCounting (sequence: RDD[String], sparkContext: SparkContext, k:Broadcast[Int]) {
  import SeqKmerCounting._

  private val intermediateCount = kmerExtraction(sequence, k)

  def canonicalCounter: RDD[(String, Int)] =
    counting(intermediateCount, sparkContext, true)

  def nonCanonicalCounter: RDD[(String, Int)] =
    counting(intermediateCount, sparkContext, false)

}