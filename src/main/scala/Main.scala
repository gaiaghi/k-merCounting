import counting.{CountingAlgorithm, ParKmerCounting, SeqKmerCounting}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.SparkContextSetup


object Main {

  private def _invokeCounting(genSeq: RDD[String], sc: SparkContext, k:Broadcast[Int],
                              countingType: String, exeMode:String): (List[RDD[(String, Int)]],Double)={

//    if (exeMode == "sequential") {
      val res = countingType match {
        case "canonical" => {
          val startTime = System.nanoTime
          val counter = new SeqKmerCounting(genSeq, sc, k)
          val kmers = counter.canonicalCounter
          val exeTime = (System.nanoTime - startTime) / 1e9d
          (List(kmers), exeTime)
        }
        case "non-canonical" =>{
          val startTime = System.nanoTime
          val counter = new SeqKmerCounting(genSeq, sc, k)
          val kmers = counter.nonCanonicalCounter
          val exeTime = (System.nanoTime - startTime) / 1e9d
          (List(kmers), exeTime)
        }
        case "both" =>{
          val startTime = System.nanoTime
          val counter = new SeqKmerCounting(genSeq, sc, k)
          val canonicalKmers = counter.canonicalCounter
          val kmers = counter.nonCanonicalCounter
          val exeTime = (System.nanoTime - startTime) / 1e9d
          (List(canonicalKmers,kmers), exeTime)
        }
      }

      res

//    }
//    else{
//      val counter: CountingAlgorithm = ParKmerCounting
//    }
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
    val fileName = if (args.length > 1) args(1) else "data/sample.fna"//TODO change to "data/humantest.fna"
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
    //removing comment lines
    val filteredGenSeq = genSeq.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";")
        )
    })

    //TODO dai la possibilità di svoglere il counting su più valori di k contemporaneamente?
    val broadcastK: Broadcast[Int] = sparkContext.broadcast(kLen.toInt)

    //execute k-mer counting
    val results = _invokeCounting(filteredGenSeq, sparkContext, broadcastK, "non-canonical", exeMode) //TODO countingType invece di "both"

    //stampa di prova:
//    results._1.map(_.toString())
    

    //TODO save the results


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