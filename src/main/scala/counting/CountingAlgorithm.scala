package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

trait CountingAlgorithm {
  def counting(sequence: RDD[String], sparkContext: SparkContext, k:Broadcast[Int]): RDD[(String,Int)]

  //TODO da implementare negli oggetti
  def toString: String
}