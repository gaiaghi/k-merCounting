package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

trait CountingAlgorithm {

  type T
  def kmerExtraction(sequence: RDD[String], k:Broadcast[Int]): T
  def counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)]

  //TODO da implementare negli oggetti
  def toString: String
}