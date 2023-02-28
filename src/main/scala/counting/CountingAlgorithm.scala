package counting

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

abstract class CountingAlgorithm ( val fileName: String, val sparkContext: SparkContext,
                                   val k:Broadcast[Int]) {

  type T  //extracted kmers
  type S  //sequence type
  val sequence: S
  def _kmerExtraction(k:Broadcast[Int]): T
  def _counting(kmers: T, canonical: Boolean): RDD[(String, Int)]

  //TODO da implementare negli oggetti
//  def toString: String

  def canonicalCounter: RDD[(String, Int)]
  def nonCanonicalCounter: RDD[(String, Int)]

}