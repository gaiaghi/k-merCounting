package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class NGramCounting(sequence: RDD[String], sparkContext: SparkContext,
                    k:Broadcast[Int]) extends CountingAlgorithm(sequence, sparkContext, k)  with CountingType {

  //    import sparkSession.implicits._
  //    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", ">")
  //    val df = sparkSession.read.option("delimiter", ">").textFile(fileName)
  //    df.show(1)
  //    println(df.count())
  //    val df2 = df.map(line => line.split("(?=>)"))
  //    //split the FASTA file into entries (genomflatMap(ic subsequence), finding each ">" header
  //    val filteredSeq = {
  //      df2.flatMap(
  //        par => par.map(str => str.split("\n").filter(line => !line.startsWith(">"))
  //        ).filter(arr => arr.nonEmpty))
  //    }
  //    val baseMap: Map[Char, Char] = Map('a' -> 'A', 't' -> 'T', 'c' -> 'C', 'g' -> 'G',
  //      'A' -> 'A', 'T' -> 'T', 'C' -> 'C', 'G' -> 'G').withDefaultValue('N')
  //    val complementMap: Map[Char, Char] = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')
  //    def transformBases(seq: String): String = {
  //      seq map baseMap
  //    }
  //    val entries = filteredSeq.map(str => str.map(s => transformBases(s)))
  //    val prova = entries.map(str => str.flatMap(_.split("")))
  //    val ngrammer = new NGram().setN(3).setInputCol("value").setOutputCol("ngrams")
  //    val libKmers = ngrammer.transform(prova)
  override type T = this.type

  override def _kmerExtraction(sequence: RDD[String], k: Broadcast[Int]): NGramCounting.this.type = ???

  override def _counting(kmers: NGramCounting.this.type, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = ???
}
