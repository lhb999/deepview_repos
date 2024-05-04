package Master

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD

object ROOTINFO extends  Serializable {
  var root = None: Option[Node]
  val partitionNumbersArr = ArrayBuffer[LeafNode]() //
  val partitionDim = Map[Int,LeafNode]() //mbr 재계산할때 파티션 번호/leafnode
}

object RDDROOTINFO extends Serializable {
  var kdtree_leafnode_number = 4 //이거 입력값으로 변경가능하게 해야대
  var rddArr = new Array[Option[RddInternalNode]](kdtree_leafnode_number)
  val partitionNumbersArr = ArrayBuffer[RddLeafNode]()
  val partitionDim = Map[Int,RddLeafNode]() //mbr 재계산할때 파티션 번호/leafnode
}

class queryRange extends  Serializable {
  val dimensionDist = Map[Int,Double]()
  var sumRange=0.0
}

class HDIClass extends  Serializable {
  var file_name=""
  var split_number = 0

  val dimension = 128
  val ratio = 0.1
  val mbr = new Array[MBR](dimension) 
  var quant_value = 0
  val featureSize = 128
  var data_number =0
  var voc=0

  var partition_number = 0
  var kdtree_leafnode_number = 0
}

case class MBR(var max:Int=255,var min:Int=0)






