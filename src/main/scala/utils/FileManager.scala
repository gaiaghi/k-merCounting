package utils

import org.apache.spark.rdd.RDD

import java.io.{FileWriter, PrintWriter}

object FileManager{

  /*
  * Save results to local file
  * */
  def writeResults(path:String, countingType:String, results:(List[RDD[(String, Int)]], Double)
                  ): Unit = {
    val pw = new PrintWriter(new FileWriter(path))

    val toPrint = if (countingType == "non-canonical") {
      List("\n> Non-canonical k-mers:").zip(results._1)
    }
    else {
      List("\n> Canonical k-mers:", "\n> Non-canonical k-mers:").zip(results._1)
    }

    pw.println("Execution time: " + results._2 + "sec.\n")
    toPrint.foreach(m => (pw.println(m._1), m._2.collect().foreach(k => (pw.print("(" + k._1 + ", "), pw.println(k._2 + ")")))))

    pw.close()
  }

}