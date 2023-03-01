package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.rdd.RDDFunctions.fromRDD
import org.apache.spark.rdd.RDD
import utils.FileManager
import utils.GenomicUtils.{reverseComplement, transformBases}


class ParKmerCounting(fileName: String, sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(fileName, sparkContext, k) with CountingType {

  override type T = RDD[(String,Int)]
  override type S = RDD[String]

  //read the FASTA file
  override val sequence: S = FileManager.readFASTAtoRDD(fileName, sparkContext)

  override def _kmerExtraction( k: Broadcast[Int]): T = {
    val seq = sequence.flatMap(line => line.split("(?=>)"))

    //remove header lines (starting with ">") and split the collection
    val filteredSeq = seq.filter(line => !line.startsWith(">")).flatMap(_.split(""))

    //translate genomic bases
    val entries = filteredSeq.map(transformBases)

    //extracting kmers
    //TODO make persistent? o forse nella funzione che chiama poi quest'altra funzione?
    //TODO usare mapPartition?
    val kmers = entries.sliding(k.value,1).map(str => str.mkString("")).filter(!_.contains("N")).map((_,1))

    kmers
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