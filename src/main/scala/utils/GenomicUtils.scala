package utils

object GenomicUtils {
  private val baseMap: Map[Char, Char] = Map('a' -> 'A', 't' -> 'T', 'c' -> 'C', 'g' -> 'G',
    'A' -> 'A', 'T' -> 'T', 'C' -> 'C', 'G' -> 'G').withDefaultValue('N')


  private val complementMap: Map[Char, Char] = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')

  def transformBases(seq: String): String = {
    /*
    * Apply characters translation to keep only the interested bases in the genomic sequence.
    * */
    seq map baseMap
  }

  def reverseComplement(seq: String): String = {
    /*
    * Computes the reverse complement of a genomic sequence, and returns the smallest sequence (alphabetically).
    * */
    val reverse = seq.reverse map complementMap

    if (reverse.compare(seq) > 0) { seq }
    else { reverse }
  }

}