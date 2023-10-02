package counting
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.rdd.RDDFunctions.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.{FileManager, SparkContextSetup}
import utils.GenomicUtils.{reverseComplement, transformBases}


class ParKmerCounting(fileName: String, sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(fileName, sparkContext, k) with CountingType {

  override type T = RDD[(String,Int)]
  override type S = RDD[String]

  //read the FASTA file
  override val sequence: S = FileManager.readFASTAtoRDD(fileName, sparkContext)
  override val kmers: T = _kmerExtraction(k)

  override def _kmerExtraction( k: Broadcast[Int]): T = {

    val seq = sequence

    //remove header lines (starting with ">") and split the collection
    val filteredSeq = seq.filter(line => !line.startsWith(">")).flatMap(_.split(""))
    //translate genomic bases
    val entries = filteredSeq.map(transformBases)

    //extracting kmers
    val kmers = entries.sliding(k.value,1).map(str => str.mkString("")).filter(!_.contains("N")).map((_,1))
    val partitionedKmers = kmers.partitionBy(new RangePartitioner(SparkContextSetup.PARTITIONS, kmers))
      .persist(StorageLevel.MEMORY_AND_DISK)
    partitionedKmers
  }

  override def _counting(kmers: T, canonical: Boolean): RDD[(String, Int)] = {

    val kmerGroupped: RDD[(String, Int)] = {
      if (canonical){
        kmers.map{case (k,v) => ( reverseComplement(k),v)}.reduceByKey(_+_)
      } else {
        kmers.reduceByKey(_+_)
      }
    }

    kmerGroupped
  }

}