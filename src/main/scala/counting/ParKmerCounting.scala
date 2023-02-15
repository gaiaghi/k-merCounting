package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.GenomicUtils.transformBases

class ParKmerCounting(sequence: RDD[String], sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(sequence, sparkContext, k) with CountingType {

  override type T = RDD[(String,Int)]

  override def _kmerExtraction(sequence: RDD[String], k: Broadcast[Int]): T = {
    val seq = sequence.map(line => line.split("(?=>)"))

//    val seq = sequence.collect().mkString("\n").split("(?=>)")
//    //split the FASTA file into entries (genomic subsequence), finding each ">" header
//    seq.flatMap(str => str.split("\n").filter(line => !line.startsWith(">")))
//    //translate genomic bases
//    val entries = seq.map(str => transformBases(str))
//    //extracting kmers
//    entries.flatMap(_.sliding(k.value, 1).filter(kmer => !kmer.contains("N")).map((_, 1)))
  }

  override def _counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = ???
}