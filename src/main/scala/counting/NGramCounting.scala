package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class NGramCounting(sequence: RDD[String], sparkContext: SparkContext,
                    k:Broadcast[Int]) extends CountingAlgorithm(sequence, sparkContext, k)  with CountingType {

  override type T = this.type

  override def _kmerExtraction(sequence: RDD[String], k: Broadcast[Int]): T = ???

  override def _counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = ???

  //-------------------------------------------------------------
//
//  import sparkSession.implicits._
//
//  //    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", ">")
//  //    val df = sparkSession.read.option("delimiter", ">").textFile(fileName)
//  val df2 = sparkSession.read.option("lineSep", ">").textFile(fileName)
//  println("ultima prova, davvero: " + df2.count())
//  println(df2.show(5))
//  val dff2 = df2.withColumn("value", regexp_replace($"value", "^[a-zA-Z].+\\n", ""))
//  println(dff2.show(5))
//  //TODO dff2 sembra andare bene come suddivisione degli entires/sequence reads.
//  // adesso devo provare a fare in modo che ogni riga diventi un Array e quindi applicare tutte le conversioni eccetera
//
//
//  val df = sparkSession.read.textFile(fileName)
//  //    df.show(1)
//
//  //per controllare che abbia trovato il numero giusto di entries
//  //    def countAll(pattern: String) = udf((s: String) => pattern.r.findAllIn(s).size)
//  //    val dfff = df.withColumn("count", countAll(">")($"value"))
//  //    println(dfff.select(col("count")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_))
//
//
//  //    val df2 = df.map(line => line.split("(?=>)"))
//  //split the FASTA file into entries (genomflatMap(ic subsequence), finding each ">" header
//  //    val filteredSeq = {
//  //      df2.flatMap(
//  //        par => par.map(str => str.split("\n").filter(line => !line.startsWith(">"))
//  //        ).filter(arr => arr.nonEmpty))
//  //    }
//
//  //    val rdd2 = rdd.flatMap(_.split("\\^\\*~")).map(_.split("\\^\\|\\&") match {
//  //      case Array(a, b, c, d, e) => (a, b, c, d, e)
//  //    })
//
//
//  val filteredSeq = df.filter(line => {
//    !(
//      line.startsWith("@") ||
//        line.startsWith("+") ||
//        line.startsWith(";") ||
//        line.startsWith(">") ||
//        line.isEmpty
//      )
//  })
//  //    println(filteredSeq.count())
//  val baseMap: Map[Char, Char] = Map('a' -> 'A', 't' -> 'T', 'c' -> 'C', 'g' -> 'G',
//    'A' -> 'A', 'T' -> 'T', 'C' -> 'C', 'G' -> 'G').withDefaultValue('N')
//  val complementMap: Map[Char, Char] = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')
//
//  def transformBases(seq: String): String = {
//    seq map baseMap
//  }
//
//  def reverseComplement(seq: String): String = {
//    val reverse = seq.reverse map complementMap
//    if (reverse.compare(seq) > 0) {
//      seq
//    } else {
//      reverse
//    }
//  }
//
//  val entries = filteredSeq.map(s => transformBases(s).split("\n"))
//  val prova = entries.map(str => str.flatMap(_.split("")))
//
//  val ngrammer = new NGram().setN(3).setInputCol("value").setOutputCol("ngrams")
//  val libKmers = ngrammer.transform(prova)
//  //    println(libKmers.show(10))
//  //    libKmers.select("ngrams").show(10)
//
//  val mah = libKmers.select(explode(col("ngrams")).alias("ngrams"))
//  val mahh = mah.map(r => r.mkString.replace(" ", "")).filter(kmer => !kmer.contains("N"))
//  println(mahh.show(10))
//  val mahhh = mahh.map(kmer => reverseComplement(kmer)).groupBy("value").count()
//  println(mahhh.show())

  //-------------------------------------------------------------


}
