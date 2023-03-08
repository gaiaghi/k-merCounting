import counting.{CountingAlgorithm, NGramCounting, ParKmerCounting, SeqKmerCounting}
import utils.FileManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.SparkContextSetup


object Main {

  private def _invokeCounting(fileName: String, sc: SparkContext, ss: SparkSession, k:Broadcast[Int],
                              countingType: String, exeMode:String): (List[Array[(String, Int)]],Double)={

//    val counter:CountingAlgorithm =
//      if (exeMode == "sequential") { new SeqKmerCounting(genSeq, sc, k)}
//      else { new ParKmerCounting(genSeq, sc, k) }

    val counter: CountingAlgorithm = exeMode match {
      case "sequential" =>
        new SeqKmerCounting(fileName, sc, k)

      case "parallel" =>
        new ParKmerCounting(fileName, sc, k)

      case "library" =>
        new NGramCounting(fileName, sc, ss, k)
    }

    val res = countingType match {
      case "canonical" =>
        val startTime = System.nanoTime
        val kmers = counter.canonicalCounter.collect()
        val exeTime = (System.nanoTime - startTime) / 1e9d
        (List(kmers), exeTime)

      case "non-canonical" =>
        val startTime = System.nanoTime
        val kmers = counter.nonCanonicalCounter.collect()
        val exeTime = (System.nanoTime - startTime) / 1e9d
        (List(kmers), exeTime)
      case "both" =>
        val startTime = System.nanoTime
        val canonicalKmers = counter.canonicalCounter.collect()
        val kmers = counter.nonCanonicalCounter.collect()
        //TODO mettere un counter.bothCounter che fa entrambi i casi in una chiamata? + persistent data
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
    * args(5) = execution mode ("parallel", "sequential", "library"). Default: sequential
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
        case "human" => "data/GCF_000001405.40_GRCh38.p14_genomic_homo_sapiens.fna.gz"
        case _ => "data/humantest.fna"
      }
    } else "data/humantest.fna"
    val kLen = if (args.length > 2) args(2) else "3" //TODO controlla i valori k dei kmer più usati
    val countingType = if (args.length > 3 && (args(3) == "canonical" || args(3) == "both")) args(3) else "non-canonical"
    val parallelism = if (args.length > 4) args(4) else "4"
//    val exeMode = if (args.length > 5 && args(5) == "parallel") args(5) else "sequential"
    val exeMode = if (args.length > 5){
      args(5) match {
        case "parallel" | "sequential" | "library" => args(5)
        case _ => "sequential"
      }
    } else "sequential"

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


    val broadcastK: Broadcast[Int] = sparkContext.broadcast(kLen.toInt)


    //execute k-mer counting
    val startTime = System.nanoTime
    val results = _invokeCounting(fileName, sparkContext, sparkSession, broadcastK, countingType, exeMode)
    val exeTime = (System.nanoTime - startTime) / 1e9d

    println("K-mer counting computed in "+results._2+ " sec. ")
    println("Saving results in file...")

    //TODO controlla come fare nel caso del cloud
    //save the results
    val outPath = "output/results.txt"
    FileManager.writeResults(outPath, countingType, results)
    println("Results saved in "+outPath+" file.")

    //TODO
    //  3. riprova con homo sapiens, quando hai il timing funzionante
    //  4. ottimizza per spark

    sparkSession.stop()
  }
}