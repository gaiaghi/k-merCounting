import utils.SparkContextSetup


object Main {

  def main(args: Array[String]): Unit = {
    /*
    * args(0) = master
    * args(1) = path of the fasta file
    * args(2) = parallelism
    * args(3) = execution mode (parallel / sequential)
    * args(4) = canonical or non-canonical counting
    * */

    //for local only
//    System.setProperty("hadoop.home.dir", "C:\\Users\\Utente\\winutils\\hadoop-3.2.2")

    //read arguments
    val master = args(0)
    val fileName = if (args.length > 1) args(1) else "data/humantest.fna"
    val parallelism = if (args.length > 2) args(2) else "1"
    val exeMode = if (args.length > 3 && args(3) == "parallel") args(3) else "sequential"
    val countingType = if (args.length > 4 && args(4) == "canonical") args(4) else "non-canonical"

    //creating spark session
    val sparkSession = SparkContextSetup.sparkSession(master, parallelism.toInt)
    val sparkContext = sparkSession.sparkContext

    //chosed configuration
    println("Current configuration:")
    println("\t- master: " + master)
    println("\t- data path: " + fileName)
    println("\t- counting type: " + countingType)
    println("\t- execution mode: "+ exeMode)
    println("\t- Spark Context initialized with parallelism: " + parallelism)


    //loading the fasta file
    val genSeq = sparkContext.textFile(fileName)

    
    //prova veloce
    // ___________________________________________________________
    val input = "data/sample.fna"
    val K = 2
    val broadcastK = sparkContext.broadcast(K)
    val records = sparkContext.textFile(input)
    // remove the records, which are not an actual sequence data

    val filteredRDD = records.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";") ||
          line.startsWith(">")
        )
    })
    filteredRDD.collect().foreach(println)
    val kmers = filteredRDD.flatMap(_.sliding(broadcastK.value, 1).map((_, 1)))
    kmers.collect().foreach(println)

    // find frequencies of kmers
    val kmersGrouped = kmers.reduceByKey(_ + _)

    kmersGrouped.saveAsTextFile("output1.txt")

    //___________________________________________________________

  }
}