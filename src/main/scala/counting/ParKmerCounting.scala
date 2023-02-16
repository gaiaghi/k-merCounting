package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.GenomicUtils.{reverseComplement, transformBases}

class ParKmerCounting(sequence: RDD[String], sparkContext: SparkContext,
                      k:Broadcast[Int]) extends CountingAlgorithm(sequence, sparkContext, k) with CountingType {

  override type T = RDD[(String,Int)]

  override def _kmerExtraction(sequence: RDD[String], k: Broadcast[Int]): T = {
    val seq = sequence.map(line => line.split("(?=>)"))


    //split the FASTA file into entries (genomflatMap(ic subsequence), finding each ">" header
    val filteredSeq = {
      seq.flatMap(
        par => par.map(str => str.split("\n").filter(line => !line.startsWith(">"))
        ).filter(arr => arr.nonEmpty))
    }

    //translate genomic bases
    val entries = filteredSeq.map( str => str.map(transformBases))

    //extracting kmers
    //TODO make persistent? o forse nella funzione che chiama poi quest'altra funzione?
    val kmers = entries.flatMap(str => str.flatMap(
      sec => sec.sliding(k.value,1).filter(kmer => !kmer.contains("N")) ).map((_,1))
    )

//    kmers.collect().foreach(println)

//    val kmers = filteredRDD.flatMap(_.sliding(broadcastK.value, 1).map((_, 1)))
//    // find frequencies of kmers
//    val kmersGrouped = kmers.reduceByKey(_ + _)

    kmers
  }

  override def _counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = {

    val kmerGroupped: RDD[(String, Int)] = {
      if (canonical){
        kmers.map{case (k,v) => ( reverseComplement(k),v)}.reduceByKey(_+_)
      } else {
        kmers.reduceByKey(_+_)
      }
    }

    kmerGroupped.collect().foreach(println)
    kmerGroupped

    //    val kmersGroupped: Map[String, Int] =
//      if (canonical) {
//        kmers.groupBy(_._1).map { case (k, v) => k -> v.map {
//          _._2
//        }.sum
//        }
//      }
//      else {
//        kmers.groupBy(kmer => reverseComplement(kmer._1)).map { case (k, v) => k -> v.map {
//          _._2
//        }.sum
//        }
//      }
  }
}