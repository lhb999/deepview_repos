// 사용방법 var y = orginaldata.partitionBy(new kdtreePartitione(spi.split_number))//

package Master
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

import scala.annotation.tailrec

object searchNode extends Serializable {
  var splitDim = 0
  var realData =0

  @tailrec
  def searchLeaf(node:Option[Node],data:(Array[Double],Array[String])):Int= { //data는 문서상의 realdata를 가지고 있다.
    node.get match {
      case _: InternalNode =>
        val inode = node.get.asInstanceOf[InternalNode]
        val splitVal=inode.split_val
        splitDim = inode.split_dim
        realData = data._1(splitDim).toInt

        if (realData <= splitVal)
          searchLeaf (Some(inode.left), data)
        else
          searchLeaf (Some(inode.right), data)

      case _: LeafNode =>
        node.get.asInstanceOf[LeafNode].partitionNumber
    }
  }
}

class kdtreePartitioner(override val numPartitions: Int) extends Partitioner {
  val node = main.broadKd.value.root
  def getPartition(key: Any): Int = {
     val data = key.asInstanceOf[(Array[Double],Array[String])] //이렇게 안하면 오류남 (넘겨서 변경하면 오류나므로 미리 변경해서 넘겨줌)
     searchNode.searchLeaf(node,data) % numPartitions
  }
}


