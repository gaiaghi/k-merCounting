import counting.{CountingAlgorithm, ParKmerCounting, SeqKmerCounting}
import org.apache.spark.broadcast.Broadcast
import utils.SparkContextSetup


object Main {

  def main(args: Array[String]): Unit = {
    /*
    * args(0) = master
    * args(1) = path of the fasta file. Default: data/humantest.fna
    * args(2) = k value (length of the kmers). Default: //TODO scrivi i default values
    * args(3) = counting type ("canonical", "non-canonical", "both"). Default: "both"
    * args(4) = parallelism. Default: 4
    * args(5) = execution mode ("parallel", "sequential"). Default: sequential
    * */

    //for local only
//    System.setProperty("hadoop.home.dir", "C:\\Users\\Utente\\winutils\\hadoop-3.2.2")

    //read arguments
    val master = args(0)
    val fileName = if (args.length > 1) args(1) else "data/sample.fna"//TODO change to "data/humantest.fna"
    val kLen = if (args.length > 2) args(2) else "3" //TODO controlla i valori k dei kmer più usati
    val countingType = if (args.length > 3 && args(3) == "canonical") true else false
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
    //removing comment lines
    val filteredGenSeq = genSeq.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";")
        )
    })

    //select counting algorithm (sequential/parallel)
    val counter : CountingAlgorithm = exeMode match  {
      case "sequential" => SeqKmerCounting
      case "parallel" => ParKmerCounting
    }

    //TODO dai la possibilità di svoglere il counting su più valori di k contemporaneamente
    val broadcastK: Broadcast[Int] = sparkContext.broadcast(kLen.toInt)

    //execute k-mer counting
    counter.counting(filteredGenSeq, sparkContext, broadcastK, countingType)


    //prova veloce
    // ___________________________________________________________
//    val input = "data/sample.fna"
//    val K = 3
//    val broadcastkK = sparkContext.broadcast(K)
//    val records = sparkContext.textFile(input)
//    // remove the records, which are not an actual sequence data
//    val filteredRDD = records.filter(line => {
//      !(
//        line.startsWith("@") ||
//          line.startsWith("+") ||
//          line.startsWith(";") ||
//          line.startsWith(">")
//        )
//    })
//
//    filteredRDD.collect().foreach(println)
//    val kmers = filteredRDD.flatMap(_.sliding(broadcastkK.value, 1).map((_, 1)))
////    kmers.collect().foreach(println)
//
//    // find frequencies of kmers
//    val kmersGrouped = kmers.reduceByKey(_ + _)
//    kmersGrouped.foreach(println)
////    kmersGrouped.saveAsTextFile("output1.txt")

    //___________________________________________________________

  }
}