package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.GenomicUtils._

class SeqKmerCounting(sequence: RDD[String], sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(sequence, sparkContext, k) with CountingType {
  override type T = Array[(String,Int)]

  override def _kmerExtraction(sequence: RDD[String], k:Broadcast[Int]): T = {

    val seq = sequence.collect().mkString("\n").split("(?=>)")

    //split the FASTA file into entries (genomic subsequence), finding each ">" header
    seq.flatMap(str => str.split("\n").filter(line => !line.startsWith(">")))

    //translate genomic bases
    val entries = seq.map(str => transformBases(str))

    //extracting kmers
    entries.flatMap(_.sliding(k.value, 1).filter(kmer => !kmer.contains("N")).map((_, 1)))
  }

  override def _counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = {

      val kmersGroupped: Map[String, Int] =
        if (canonical) {
          kmers.groupBy(kmer => reverseComplement(kmer._1)).map { case (k, v) => k -> v.map {_._2}.sum }
        }
        else {
          kmers.groupBy(_._1).map { case (k, v) => k -> v.map {_._2}.sum }
        }
//      kmersGroupped.foreach(println)
      sparkContext.parallelize(kmersGroupped.toSeq)
  }

}


//class SeqKmerCounting (sequence: RDD[String], sparkContext: SparkContext, k:Broadcast[Int]){
//  import SeqKmerCounting._
//
//  def intermediateCount: T = kmerExtraction(sequence, k)
//
//  def canonicalCounter: RDD[(String, Int)] =
//    counting(intermediateCount, sparkContext, canonical = true)
//
//  def nonCanonicalCounter: RDD[(String, Int)] =
//    counting(intermediateCount, sparkContext, canonical = false)
//
//}