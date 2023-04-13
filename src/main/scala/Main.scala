import counting.{CountingAlgorithm, NGramCounting, ParKmerCounting, SeqKmerCounting}
import utils.FileManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import utils.SparkContextSetup


object Main {

  private def _invokeCounting(fileName: String, sc: SparkContext, ss: SparkSession, k:Broadcast[Int],
                              countingType: String, exeMode:String): (List[Array[(String, Int)]],Double)={

    val counter: CountingAlgorithm = exeMode match {
      case "sequential" =>
        new SeqKmerCounting(fileName, sc, k)

      case "distributed" =>
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
        val exeTime = (System.nanoTime - startTime) / 1e9d
        (List(canonicalKmers,kmers), exeTime)
    }

    res

  }

  def main(args: Array[String]): Unit = {
    /*
    * args(0) = master
    * args(1) = path of the fasta file. Default: data/humantest.fna
    * args(2) = k value (length of the kmers). Default: 5
    * args(3) = counting type ("canonical", "non-canonical", "both"). Default: "non-canonical"
    * args(4) = partitions. Default: 4
    * args(5) = execution mode ("distributed", "sequential", "library"). Default: sequential
    * args(6) = ouput file path. Default: "output_dir"
    * */

    //read arguments
    val master = "local"
    try {
      val master = args(0)
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        println("Specify the correct parameters: \n" +
          "    1- master\n    2- path of the fasta file\n    3- k value (length of the kmers). Default: 5\n    4- counting type (\"canonical\", \"non-canonical\", \"both\"). Default: non-canonical\n" +
          "    5- partitions. Default: 4\n    6- execution mode (\"distributed\", \"sequential\", \"library\"). Default: sequential\n" +
          "    7- ouput file path. Default: \"output_dir\"")
        System.exit(1)
    }

    val fileName = if (args.length > 1) {
      args(1) match {
        //local only shortcuts:
        case "saccharomyces" => "data/GCF_000146045.2_R64_genomic_Saccharomyces_cerevisiae.fna.gz"
        case "drosophila" => "data/GCF_000001215.4_Release_6_plus_ISO1_MT_genomic_drosophila_melanogaster.fna.gz"
        case "test" => "data/sample.fna"
        case "human" => "data/GCF_000001405.40_GRCh38.p14_genomic_homo_sapiens.fna.gz"
        case _ => "data/humantest.fna"
      }
    } else "data/humantest.fna"
    val kLen = if (args.length > 2) args(2) else "5"
    val countingType = if (args.length > 3 && (args(3) == "canonical" || args(3) == "both")) args(3) else "non-canonical"
    val parallelism = if (args.length > 4) args(4) else "4"
    val exeMode = if (args.length > 5){
      args(5) match {
        case "distributed" | "sequential" | "library" => args(5)
        case _ => "sequential"
      }
    } else "sequential"
    val outPath = if (args.length > 6) args(6) else "output_dir"

    //creating spark session
    val sparkSession = SparkContextSetup.sparkSession(master, parallelism.toInt)
    val sparkContext = sparkSession.sparkContext

    //chosed configuration
    println("Current configuration:")
    println("\t- master: " + master)
    println("\t- data path: " + fileName)
    println("\t- output path: " + outPath)
    println("\t- counting type: " + countingType)
    println("\t- execution mode: "+ exeMode)
    println("\t- Spark Context initialized with parallelism: " + parallelism + "\n")


    val broadcastK: Broadcast[Int] = sparkContext.broadcast(kLen.toInt)


    //execute k-mer counting
    val results = _invokeCounting(fileName, sparkContext, sparkSession, broadcastK, countingType, exeMode)

    println("K-mer counting computed in "+results._2+ " sec. ")
    println("Saving results in file...")

    //save the results
    FileManager.writeResults(outPath, countingType, exeMode, fileName, results, sparkContext)
    println("Results saved in "+outPath+".")

    sparkSession.stop()
  }
}