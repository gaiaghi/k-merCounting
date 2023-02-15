package counting

import org.apache.spark.rdd.RDD

trait CountingType extends CountingAlgorithm {

  override def canonicalCounter: RDD[(String, Int)] =
    _counting(_kmerExtraction(sequence, k), sparkContext, canonical = true)

  override def nonCanonicalCounter: RDD[(String, Int)] =
    _counting(_kmerExtraction(sequence, k), sparkContext, canonical = false)
}