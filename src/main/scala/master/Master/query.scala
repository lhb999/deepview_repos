package Master
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import Slave._


class kdTreeRangeQuery{
  val partitionNumbersArr = mutable.ArrayBuffer[Int]()
  val queue = new Queue[Some[Node]]

  def retrivalKdtree(node:Option[Node],range:Int,queryData:Array[Int]):Array[Int]= {
    node.get match {
      case _: InternalNode =>
        val inode    = node.get.asInstanceOf[InternalNode]
        val splitDim = inode.split_dim
        val qp       = queryData(splitDim)

        calRange(inode.left, inode, qp, splitDim, range)
        calRange(inode.right, inode, qp, splitDim, range)
        if(queue.isEmpty) {
          //          println("Number of partitions : "+partitionNumbersArr.size)
          partitionNumbersArr.toArray
        }
        else {
          retrivalKdtree(queue.dequeue(), range, queryData)
        }
        //retrivalKdtree(queue.dequeue(),range,queryData)

      case _: LeafNode =>
        val leafNode = node.get.asInstanceOf[LeafNode].partitionNumber
        partitionNumbersArr += leafNode

        if(queue.isEmpty) {
//          println("Number of partitions : "+partitionNumbersArr.size)
          partitionNumbersArr.toArray
        }
        else {
          retrivalKdtree(queue.dequeue(), range, queryData)
        }
    }
  }

  def calRange(
                node     : Node,
                inode    : InternalNode,
                qp       : Int,
                splitDim : Int,
                range    : Int
              )={
    var flag=0 //internalnode확인용
    var min=0
    var max=0

    node match { //internal과 leaf node가 min,max를 가지고 있는 방식이 다르므로 구분하여야 함.
      case _: InternalNode =>
        min = node.asInstanceOf[InternalNode].min
        max = node.asInstanceOf[InternalNode].max

      case _: LeafNode =>
        min = node.asInstanceOf[LeafNode].mbr(splitDim).min
        max = node.asInstanceOf[LeafNode].mbr(splitDim).max
    }

    val dimDistance  = calDist(min, max, qp) // 노드의 차원의 거리
    val SumRange     = inode.asInstanceOf[InternalNode].range.sumRange //이전까지 더한 range
    val currentRange = SumRange + dimDistance

    if(currentRange <= range) { //range를 넘지 않으면 queue에 후보로 담아준다.
      inode.range.dimensionDist += (splitDim -> dimDistance) //같은 차원에 대한 계산이면 추가를 하는게 아니라 업데이트를 해야한다.
      val InternalNodeRange      = inode.asInstanceOf[InternalNode].range //range에는 차원에 대한 range MAP 구조 dimensionDist와 트리를 타면서 계산한 sumRange가있다
      InternalNodeRange.sumRange = InternalNodeRange.dimensionDist.foldLeft(0.0)(_+_._2) //업데이트 된 결과를 다시 합산한다. 그니까 지금까지 지나온 차원의 range를 모두 더한다.
      queue.enqueue(Some(node))
    }
  }

  def calDist(
               min : Int ,
               max : Int,
               qp  : Int
             ): Double = {
    if(qp >= min && qp <= max)
      0.0
    else {
      val calMax =  Math.sqrt(Math.pow(max-qp,2.0).abs)
      val calMin =  Math.sqrt(Math.pow(min-qp,2.0).abs)
      Math.pow(Math.min(Math.sqrt(calMax),Math.sqrt(calMin)),2.0)
    }
  }
}

class kdTreeKnnQuery{
  def knnQuery(
                k         : Int,
                queryData : Array[Int]
              ): (Map[Int,Int],Double,Int)={ //Map(파티션,k) 랑 tempRange랑 Dpo set을 위한 qPartition번호

    val rangeQuery = new kdTreeRangeQuery()
    val qPartition = getPartitionNumber(ROOTINFO.root.get,queryData) //qp가 속한 partiton 탐색
    val queryRange = setQueryRange(qPartition,k)//k/dpo를 이용해서 queryRange 계산
    val containListWithDist = rangeQuery.retrivalKdtree(
      ROOTINFO.root,
      queryRange.toInt,
      queryData
    )//새로 얻은 range를 이용해 range 쿼리 진행
    val containListWithDistMap = Map[Int,Double]() //partition번호,dist
    val partitionWithKMap      = Map[Int,Int]()//partition번호,k

    for(partitionNumber <- containListWithDist)
      containListWithDistMap +=(partitionNumber -> calDist(partitionNumber,queryData)) //파티션과 쿼리포인터사이의 거리를 구함

    val kdTreePartitioNumber = main.broadHdi.value.partition_number

    for(partition <- 0 until kdTreePartitioNumber) //slave에서 null을 방지하기 위한 목적
      partitionWithKMap +=(partition -> 0)

    for(partitionNumber <- containListWithDist){ //dpo와 dist를 이용하여 각 파티션 별로 k를 다르게 주기 위함
      val dist = containListWithDistMap(partitionNumber)
      val dpo  = ROOTINFO.partitionDim(partitionNumber).asInstanceOf[LeafNode].dpo

      var kWithRange = k-(dist*dpo).toInt
      if(kWithRange < 0) kWithRange =0
      partitionWithKMap += (partitionNumber->kWithRange)
    }
    (partitionWithKMap,queryRange,qPartition)
  }

  def getPartitionNumber(
                          node      : Node,
                          queryData : Array[Int]
                        ):Int= { //처음 쿼리포인트를 가지고 파티션을 하나 결정한다.
    node match {
      case _: InternalNode =>
        val inode    = node.asInstanceOf[InternalNode]
        val splitDim = inode.split_dim
        val splitVal = inode.split_val
        val realData = queryData(splitDim)

        if(realData <= splitVal )
          getPartitionNumber(inode.left,queryData)
        else
          getPartitionNumber(inode.right,queryData)

      case _: LeafNode =>
        node.asInstanceOf[LeafNode].partitionNumber
    }
  }

  def setQueryRange(
                     partitionNumber : Int,
                     k               : Int
                   ): Double= { //dpo/k를 이용하여 range를 계산한다.

    val dpo = ROOTINFO.partitionDim(partitionNumber).asInstanceOf[LeafNode].dpo
    k/dpo

  }

  def calDist(
               partitionNumber : Int,
               queryData       : Array[Int]
             ): Double={
    val node      = ROOTINFO.partitionDim(partitionNumber).asInstanceOf[LeafNode]
    val split_dim = node.spi_dim
    val max       = node.mbr(split_dim).max
    val min       = node.mbr(split_dim).min
    val realData  = queryData(split_dim)

    val calMax   = (Math.pow(max,2.0)- Math.pow(realData,2.0)).abs
    val calMin   = (Math.pow(min,2.0)- Math.pow(realData,2.0)).abs
    var distance = 0.0

    val rangeQuery = new kdTreeRangeQuery()

    for (dim <- 0 until main.HDI.dimension) {
      val max = node.mbr(dim).max
      val min = node.mbr(dim).min
      val queryPoint = queryData(dim)


      distance += rangeQuery.calDist(min,max,queryPoint)
    }
    distance
  }
}