package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.FileManager
import utils.GenomicUtils._
import scala.collection.parallel.CollectionConverters._

class SeqKmerCounting(fileName: String, sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(fileName, sparkContext, k) with CountingType {
  override type T = Array[(String,Int)]
  override type S = RDD[String]

  //read the FASTA file
  override val sequence: S = FileManager.readFASTAtoRDD(fileName, sparkContext)
  override val kmers: T = _kmerExtraction(k)

  override def _kmerExtraction( k:Broadcast[Int]): T = {

    //split the FASTA file into entries (genomic subsequence), and removing each ">" header
    val seq = sequence.collect().mkString("\n")

    val filteredSeq = seq.replaceAll(">.*\\n","").replaceAll("\n", "")
    //translate genomic bases
    val entries = transformBases(filteredSeq)


    //extracting kmers
    entries.sliding(k.value, 1).filter(kmer => !kmer.contains("N")).map((_, 1)).toArray

  }

  override def _counting(kmers: T, canonical: Boolean): RDD[(String, Int)] = {

      val kmersGroupped: Map[String, Int] =
        if (canonical) {
          kmers.groupBy(kmer => reverseComplement(kmer._1)).map { case (k, v) => k -> v.map {_._2}.sum }
        }
        else {
          kmers.groupBy(_._1).map { case (k, v) => k -> v.map {_._2}.sum }
        }

      sparkContext.parallelize(kmersGroupped.toSeq).sortBy(_._1)
  }

}