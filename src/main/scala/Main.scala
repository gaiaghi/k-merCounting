import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import utils.SparkContextSetup

object Main {

  def main(args: Array[String]): Unit = {
    /*
    * args(0) = master
    * args(1) = path of the fasta file
    * args(2) = parallelism
    * */

    //local only
    System.setProperty("hadoop.home.dir", "C:\\Users\\Utente\\winutils\\hadoop-3.2.2")

    //read arguments
    val master = args(0)
    val fileName = if (args.length > 1) args(1) else "data/humantest.fna"
    val parallelism = if (args.length >2) args(2) else "1"

    //creating spark session
//    val sparkconf = new SparkConf().setAppName("k-mer Counting").setMaster(master)
//    val sparkContext = new SparkContext(sparkconf)
    val sparkSession = SparkContextSetup.sparkSession(master, parallelism.toInt)
    val sparkContext = sparkSession.sparkContext


    //loading the fasta file
    val genSeq = sparkContext.textFile(fileName)

    println(genSeq)

  }
}