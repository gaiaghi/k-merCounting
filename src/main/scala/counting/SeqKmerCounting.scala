package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object SeqKmerCounting extends CountingAlgorithm {

  private val baseMap: Map[Char, Char] = Map('a' -> 'A', 't' -> 'T', 'c' -> 'C', 'g' -> 'G',
    'A' -> 'A', 'T' -> 'T', 'C' -> 'C', 'G' -> 'G').withDefaultValue('N')


  private val complementMap: Map[Char, Char] = Map ('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')


  private def _transformBases(seq:String):String = {
    /*
    * Apply characters translation to keep only the interested bases in the genomic sequence.
    * */
      seq map baseMap
  }


  private def _reverseComplement(seq: String): String = {
    /*
    * Computes the reverse complement of a genomic sequence, and returns the smallest sequence (alphabetically).
    * */
      val reverse = seq.reverse map complementMap
      if (reverse.compare(seq) > 0) { seq }  else { reverse }
  }


  override def counting(sequence: RDD[String], sparkContext: SparkContext, k:Broadcast[Int], canonical: String): RDD[(String, Int)] = {
      val seq = _transformBases(sequence.collect().mkString)

      //split the FASTA file into entries (genomic subsequence)
      val entries: Array[String] = seq.split(">")

      //extract and count the k-mers

      val kmers: Array[(String,Int)] = canonical match {
        case "canonical" =>
          entries.flatMap(_.sliding(k.value, 1).filter(kmer => !kmer.contains("N")).map(kmer => (_reverseComplement(kmer), 1)))

        case "non-canonical" =>
          entries.flatMap(_.sliding(k.value, 1).filter(kmer => !kmer.contains("N")).map((_, 1)))

        case "both" => {
          val filteredKmers = entries.flatMap(_.sliding(k.value, 1).filter(kmer => !kmer.contains("N")))
          //TODO COME RESTITUIRE DUE RISULTATI?
          filteredKmers.map((_, 1))

        }
      }

      val kmersGroupped: Map[String, Int] = kmers.groupBy(_._1).map { case (k, v) => k -> v.map {_._2}.sum }

      sparkContext.parallelize(kmersGroupped.toSeq)



    //TODO
    // - segna le tempistiche
    // - sposta le cose private ausiliari in un'altra classe?
  }
}