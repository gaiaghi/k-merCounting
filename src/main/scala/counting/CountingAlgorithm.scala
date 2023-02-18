package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class CountingAlgorithm ( val sequence: RDD[String], val sparkContext: SparkContext,
                                   val k:Broadcast[Int]) {

  type T
  def _kmerExtraction(sequence: RDD[String], k:Broadcast[Int]): T
  def _counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)]

  //TODO da implementare negli oggetti
//  def toString: String

  def canonicalCounter: RDD[(String, Int)]
  def nonCanonicalCounter: RDD[(String, Int)]

}