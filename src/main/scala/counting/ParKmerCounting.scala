package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.FileManager
import utils.GenomicUtils._

import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.mutable.ParArray

class ParKmerCounting(fileName: String, sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(fileName, sparkContext, k) with CountingType {
  override type T = ParArray[(String,Int)]
  override type S = RDD[String]

  //read the FASTA file
  override val sequence: S = FileManager.readFASTAtoRDD(fileName, sparkContext)
  override val kmers: T = _kmerExtraction(k)

  override def _kmerExtraction( k:Broadcast[Int]): T = {

    //split the FASTA file into entries (genomic subsequence), and removing each ">" header
    val seq = sequence.collect().par

    val filteredSeq = seq.filter(line => !line.startsWith(">")).flatMap(_.split(""))
    //translate genomic bases
    val entries = filteredSeq.map(transformBases)

    //extracting kmers
    entries.iterator.sliding(k.value, 1).map(str => str.mkString("")).filter(!_.contains("N")).map((_, 1)).toArray.par

  }

  override def _counting(kmers: T, canonical: Boolean): RDD[(String, Int)] = {

    val kmersGroupped: Map[String, Int] =
      if (canonical) {
        kmers.groupBy(kmer => reverseComplement(kmer._1)).map { case (k, v) => k -> v.map {_._2}.sum }.seq
      }
      else {
        kmers.groupBy(_._1).map { case (k, v) => k -> v.map {_._2}.sum }.seq
      }

    sparkContext.parallelize(kmersGroupped.toSeq).sortBy(_._1)
  }

}