package structures

class GenomicEntry (override val length: Int,
                    override  val startIndex: Int,
                    override val endIndex: Int,
                    val name: String
                   )
  extends GenomicSeq(length, startIndex, endIndex)