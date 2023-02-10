package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object SeqKmerCounting extends CountingAlgorithm {
  override def counting(sequence: RDD[String], sparkContext: SparkContext, k:Broadcast[Int]): RDD[(String, Int)] = {
      val seq = sequence.collect().mkString
//    println(seq+"\n\n")

      //split the FASTA file into entries (genomic subsequence)
      val entries: Array[String] = seq.split(">")
//      entries.foreach(println(_))
      val kmers = entries.flatMap(_.sliding(k.value,1).map((_,1)))

      val kmersGrouped = kmers.groupBy(_._1).map { case (k, v) => k -> v.map {_._2}.sum }

//      kmersGrouped.foreach(println)

    //TODO
    // -modifica il return value
    // - togli gli N
    // - versione canonica
    sequence.map((_,1))
  }
}