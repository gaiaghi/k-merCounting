import counting.{CountingAlgorithm, ParKmerCounting, SeqKmerCounting}
import utils.FileManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.NGram
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, concat, concat_ws, explode, regexp_replace, sum, udf}
import utils.SparkContextSetup


object Main {

  private def _invokeCounting(genSeq: RDD[String], sc: SparkContext, k:Broadcast[Int],
                              countingType: String, exeMode:String): (List[RDD[(String, Int)]],Double)={

    val counter:CountingAlgorithm =
      if (exeMode == "sequential") { new SeqKmerCounting(genSeq, sc, k)}
      else { new ParKmerCounting(genSeq, sc, k) }

    val res = countingType match {
      case "canonical" =>
        val startTime = System.nanoTime
        //          val counter = new SeqKmerCounting(genSeq, sc, k)
                val kmers = counter.canonicalCounter
        val exeTime = (System.nanoTime - startTime) / 1e9d
        (List(kmers), exeTime)

      case "non-canonical" =>
        val startTime = System.nanoTime
        //          val counter = new SeqKmerCounting(genSeq, sc, k)
                val kmers = counter.nonCanonicalCounter
        val exeTime = (System.nanoTime - startTime) / 1e9d
        (List(kmers), exeTime)
      case "both" =>
        val startTime = System.nanoTime
        //          val counter = new SeqKmerCounting(genSeq, sc, k)
                val canonicalKmers = counter.canonicalCounter
        val kmers = counter.nonCanonicalCounter
        val exeTime = (System.nanoTime - startTime) / 1e9d
        (List(canonicalKmers,kmers), exeTime)
    }

    res

  }

  def main(args: Array[String]): Unit = {
    /*
    * args(0) = master
    * args(1) = path of the fasta file. Default: data/humantest.fna
    * args(2) = k value (length of the kmers). Default: //TODO scrivi i default values
    * args(3) = counting type ("canonical", "non-canonical", "both"). Default: "non-canonical"
    * args(4) = parallelism. Default: 4
    * args(5) = execution mode ("parallel", "sequential"). Default: sequential
    * */

    //for local only
//    System.setProperty("hadoop.home.dir", "C:\\Users\\Utente\\winutils\\hadoop-3.2.2")

    //read arguments
    val master = args(0)
    val fileName = if (args.length > 1) {
      args(1) match {
        case "saccharomyces" => "data/GCF_000146045.2_R64_genomic_Saccharomyces_cerevisiae.fna.gz"
        case "drosophila" => "data/GCF_000001215.4_Release_6_plus_ISO1_MT_genomic_drosophila_melanogaster.fna.gz"
        case "test" => "data/sample.fna"
      }
    } else "data/humantest.fna"
    val kLen = if (args.length > 2) args(2) else "3" //TODO controlla i valori k dei kmer più usati
    val countingType = if (args.length > 3 && (args(3) == "canonical" || args(3) == "both")) args(3) else "non-canonical"
    val parallelism = if (args.length > 4) args(4) else "4"
    val exeMode = if (args.length > 5 && args(5) == "parallel") args(5) else "sequential"

    //creating spark session
    val sparkSession = SparkContextSetup.sparkSession(master, parallelism.toInt)
    val sparkContext = sparkSession.sparkContext

    //chosed configuration
    println("Current configuration:")
    println("\t- master: " + master)
    println("\t- data path: " + fileName)
    println("\t- counting type: " + countingType)
    println("\t- execution mode: "+ exeMode)
    println("\t- Spark Context initialized with parallelism: " + parallelism + "\n")


    //loading the fasta file
    val genSeq = sparkContext.textFile(fileName)

    //removing comment lines (but keeping headers ">")
    val filteredGenSeq = genSeq.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";")
        )
    })


    //TODO dai la possibilità di svoglere il counting su più valori di k contemporaneamente?
    val broadcastK: Broadcast[Int] = sparkContext.broadcast(kLen.toInt)


    //-------------------------------------------------------------
    import sparkSession.implicits._
//    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", ">")
//    val df = sparkSession.read.option("delimiter", ">").textFile(fileName)
    val df2 = sparkSession.read.option("lineSep", ">").textFile(fileName)
    println("ultima prova, davvero: "+df2.count())
    println(df2.show(5))
    val dff2 = df2.withColumn("value", regexp_replace($"value", "^[a-zA-Z].+\\n", ""))
    println(dff2.show(5))
    //TODO dff2 sembra andare bene come suddivisione degli entires/sequence reads.
    // adesso devo provare a fare in modo che ogni riga diventi un Array e quindi applicare tutte le conversioni eccetera


    val df = sparkSession.read.textFile(fileName)
//    df.show(1)

      //per controllare che abbia trovato il numero giusto di entries
//    def countAll(pattern: String) = udf((s: String) => pattern.r.findAllIn(s).size)
//    val dfff = df.withColumn("count", countAll(">")($"value"))
//    println(dfff.select(col("count")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_))


//    val df2 = df.map(line => line.split("(?=>)"))
    //split the FASTA file into entries (genomflatMap(ic subsequence), finding each ">" header
//    val filteredSeq = {
//      df2.flatMap(
//        par => par.map(str => str.split("\n").filter(line => !line.startsWith(">"))
//        ).filter(arr => arr.nonEmpty))
//    }

//    val rdd2 = rdd.flatMap(_.split("\\^\\*~")).map(_.split("\\^\\|\\&") match {
//      case Array(a, b, c, d, e) => (a, b, c, d, e)
//    })


    val filteredSeq = df.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";") ||
          line.startsWith(">") ||
          line.isEmpty
        )
    })
//    println(filteredSeq.count())
    val baseMap: Map[Char, Char] = Map('a' -> 'A', 't' -> 'T', 'c' -> 'C', 'g' -> 'G',
      'A' -> 'A', 'T' -> 'T', 'C' -> 'C', 'G' -> 'G').withDefaultValue('N')
    val complementMap: Map[Char, Char] = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')
    def transformBases(seq: String): String = {
      seq map baseMap
    }
    def reverseComplement(seq: String): String = {
      val reverse = seq.reverse map complementMap
      if (reverse.compare(seq) > 0) {seq} else {reverse}}

    val entries = filteredSeq.map(s => transformBases(s).split("\n"))
    val prova = entries.map(str => str.flatMap(_.split("")))

    val ngrammer = new NGram().setN(3).setInputCol("value").setOutputCol("ngrams")
    val libKmers = ngrammer.transform(prova)
//    println(libKmers.show(10))
//    libKmers.select("ngrams").show(10)

    val mah = libKmers.select(explode(col("ngrams")).alias("ngrams"))
    val mahh = mah.map(r => r.mkString.replace(" ","")).filter(kmer => !kmer.contains("N"))
    println(mahh.show(10))
    val mahhh = mahh.map(kmer => reverseComplement(kmer)).groupBy("value").count()
    println(mahhh.show())

    //-------------------------------------------------------------

    //execute k-mer counting
    //TODO scommenta
//    val results = _invokeCounting(filteredGenSeq, sparkContext, broadcastK, countingType, exeMode)

//    println("K-mer counting computed in "+results._2+ " sec. ")
//    println("Saving results in file...")

    //TODO controlla come fare nel caso del cloud
    //save the results
//    val outPath = "output/results.txt"
//    FileManager.writeResults(outPath, countingType, results)
//    println("Results saved in "+outPath+" file.")


    sparkSession.stop()
  }
}