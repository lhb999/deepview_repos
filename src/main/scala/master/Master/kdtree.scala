package Master

import org.apache.spark.rdd.RDD

/*kdtree 생성 */
object kdtree {
  def printArray[K](array: Array[K]) = println(array.mkString("Array(", ", ", ")"))

  def generateKdtree(quantDataRDD: RDD[Array[((Int, Int), Int)]]) = {
    ROOTINFO.root match {
      case None => //처음 tree를 생성하는 경우
        println("*********************************************************************")
        println("\t♠♠♠ KD Tree is Not Exist ...!! ....Create KD Tree.... ♠♠♠")
        println("*********************************************************************")

        println("\nINFO::Data reduce by key.....")
        val quantDataRDDforVar = quantDataRDD.flatMap(x => x).reduceByKey(_ + _) //.sortByKey()

        //quantDataRDD.map(x=>x.toList).coalesce(1).saveAsTextFile("file:////home/etri/code/log/stat_quantzation_data/")

        val (split_dim, split_quantvalue) = Quantization.calVariance(
          quantDataRDDforVar,
          main.HDI.data_number,
          null)

        val (lNodeData, rNodeData) = divideQuantizationRDD(
          quantDataRDD,
          split_dim,
          split_quantvalue
        ) //패턴 매칭은 소문자로 시작해야한다.

        val leftNode  = generateLeafNode("L", lNodeData, split_dim, split_quantvalue)
        val rightNode = generateLeafNode("R", rNodeData, split_dim, split_quantvalue)
        main.HDI.kdtree_leafnode_number = main.HDI.kdtree_leafnode_number + 2
        println("\nINFO::Tree leftnode +2 total leaftnode :" + main.HDI.kdtree_leafnode_number)

        val queryRange = new queryRange

        val realSplitValue = (split_quantvalue+1) * main.HDI.voc
        ROOTINFO.root = Some(InternalNode(
          0,
          255,
          split_dim,
          realSplitValue,
          queryRange,
          leftNode,
          rightNode)
        )


      case _ => //root가 존재함.
        println("*********************************************************************")
        println("\t♠♠♠ KD Tree is  Exist ...!! Search Divide Leaf Node... ♠♠♠")
        println("*********************************************************************")

        val search = new searchDivideLeafNode
        search.searchLeafNode(ROOTINFO.root.get) // 나누어질 노드 static이 좋을지 새로 선언이 좋을지 paraentNode(부모),location: 부모의 어디에 위치했었는지,
    }
  }

  /*분산 축 기준으로 데이터 나눔  if(qdata[spi.dim] <= spi.value) */
  def divideQuantizationRDD(PNodeData: RDD[Array[((Int, Int), Int)]], split_dim: Int, split_quantvalue: Int) = {
    println("\nINFO::Divide data with split info [spi dimension : " + split_dim + ", spi value : " + split_quantvalue + "]")

    val lNodeData = PNodeData.filter(x=>x(split_dim)._1._2 <= split_quantvalue)
    print("INFO::lNode data number :" + lNodeData.count() + "\n")

    val rNodeData = PNodeData.filter(x => x(split_dim)._1._2 > split_quantvalue)
    print("INFO::rNode data number :" + rNodeData.count() + "\n")
    (lNodeData, rNodeData)
  }
}

/*노드 생성관련 코드*/
abstract class Node
case class InternalNode(
                         min       : Int,
                         max       : Int,
                         split_dim : Int,
                         split_val : Int,
                         range     : queryRange,
                         var left  : Node = null,
                         var right : Node = null
                       ) extends Node
//인터널의 leat,right는 또다른 internal이 될수도있다.
case class LeafNode(
                     value           : RDD[Array[((Int, Int), Int)]],
                     data_number     : Int,
                     partitionNumber : Int = 0,
                     spi_dim         : Int=0,
                     var mbr         : Array[MBR] = null,
                     var dpo         : Double=0.0
                   ) extends Node

/*leafNode생성*/
object generateLeafNode {
  def apply(
             location     : String,
             nodeData     : RDD[Array[((Int, Int), Int)]],
             spi_dim      : Int,
             spi_quantval : Int,
             divideNode   : LeafNode = null
           ): LeafNode = { //location은 좌/우 어느자식인지
    var node = None: Option[LeafNode]

    if (location == "L") {
      println("\nINFO::Generate Left LeafNode")
      if (main.HDI.kdtree_leafnode_number >=  main.HDI.split_number/2){
        println("INFO::Assign a partition number to the leaf node..."+main.HDI.partition_number)
        node = calLastLeafNode(nodeData,spi_dim)
        calculationMbr(location, node.get, spi_dim,spi_quantval,divideNode)
        initDpo(node.get)
      }
      else{
        node = Some(LeafNode(nodeData, nodeData.count().toInt))
        calculationMbr(location, node.get, spi_dim,spi_quantval,divideNode)
      }
    }
    //Right Leaf Node 라면
    else {
      println("\nINFO::Generate Right LeafNode")
      if (main.HDI.kdtree_leafnode_number >= main.HDI.split_number/2){ //완전 이진 트리이기때문에 가능
        println("INFO::Assign a partition number to the leaf node..."+main.HDI.partition_number)
         //0~3사이의 값을 갖게 됨.
        node = calLastLeafNode(nodeData,spi_dim)
        calculationMbr(location, node.get, spi_dim,spi_quantval,divideNode)
        initDpo(node.get)
      }
      else{
        node = Some(LeafNode(nodeData, nodeData.count().toInt))
        calculationMbr(location, node.get, spi_dim,spi_quantval,divideNode)
      }
    }
    println("INFO::Leaf Node MBR calcuation... ")
    node.get
  }

  def calLastLeafNode(nodeData:RDD[Array[((Int, Int), Int)]],spi_dim:Int):Option[LeafNode]= {
    val node = Some(LeafNode(nodeData, nodeData.count().toInt, main.HDI.partition_number,spi_dim))
    ROOTINFO.partitionNumbersArr += node.get
    //println("INFO:: "+main.HDI.partition_number+" 에 노드를 저장했습니다.")
    ROOTINFO.partitionDim(main.HDI.partition_number)=node.get //mbr재계산용
    main.HDI.partition_number=main.HDI.partition_number+1
    node
  }

  /*leafnode mbr 계산*/
  def calculationMbr(location: String, node: LeafNode, spi_dim:Int,spi_quantval:Int,divideNode: LeafNode = null): Unit = { // root가 존재하는 경우에는 HDI의 max,min이 아니라 분할 기준이 되는 node의 max/ min 값이 되어야 함!!!
    val MbrArr = new Array[MBR](main.HDI.dimension)
    var mbr: Array[MBR] = main.HDI.mbr

    if (divideNode != null) mbr = divideNode.mbr //만약 divide가 전달되지 않았다면 tree를 첨 구성하는때이니까 HDI의 정보를 이용한다.

    if (location == "L") { // 코드가 반복되는거같아도 사실 실행하면 조건문 하나를 더 걸럴수있어서 이게 나은거같음
      for (i <- 0 until main.HDI.dimension) {
        if (i == spi_dim){
          MbrArr(i) = MBR((spi_quantval + 1) * main.HDI.voc, mbr(i).min)
          println("INFO::Right node mbr max:"+MbrArr(i).max+" min : "+MbrArr(i).min)
        }
        else
          MbrArr(i) = MBR(mbr(i).max, mbr(i).min)
      }
    } else {
      for (i <- 0 until main.HDI.dimension) {
        if(i == spi_dim) {
          MbrArr(i) = MBR(mbr(i).max, (spi_quantval + 1) * main.HDI.voc)
          println("INFO::Right node mbr max:"+MbrArr(i).max+" min : "+MbrArr(i).min)
        }
        else
          MbrArr(i) = MBR(mbr(i).max, mbr(i).min)
      }
    }
    node.mbr = MbrArr
  }

  def initDpo(partition:Node): Unit = {
    val node = partition.asInstanceOf[LeafNode]

    var sum=0.0

    for (dim <- 0 until main.HDI.dimension) {
      sum += Math.pow(node.mbr(dim).max-node.mbr(dim).min, 2.0)
    }

    val MaxDistance=Math.sqrt(sum)

    println("INFO::Node "+node.partitionNumber+"의 Maxdistance는 "+MaxDistance)
    val numberOfData = node.data_number
    val dpo = (numberOfData/MaxDistance)*2
    println("INFO::Node "+node.partitionNumber+"의 data갯수는 "+numberOfData+ " dpo는 "+dpo)
    node.dpo=dpo
  }
}

/*분할 지점 검색 완전이진트리를 구축하고 노드를 분할 하기 위한 코드*/
class searchDivideLeafNode{ //Tree Class의 root value를 전달
  var depthNum: Int = 2
  val leafNodeNum: Int = main.HDI.kdtree_leafnode_number + 1
  var before_remainder: Int = 1
  var remainder: Int = 0

  var inode    : Node = _// inode의 type을 InternaNode로 해서 left와 right를 참조할 수 있도록 해야함.
  var  node    : Node = _
  var location : String = "" //자식이 부모에게서 위치한 곳

  println("INFO::Search Divide Leaf Node.....")

  def searchLeafNode(param_header: Node): Unit = {
    //print("depthNum :"+depthNum + "leafNode Num :"+leafNodeNum+" before_remainder :"+before_remainder+"remainder :"+remainder)
    node = param_header
    remainder = leafNodeNum % depthNum

    node match {//node가 leaf일떄까지 추적한다.
      case _: InternalNode =>
        inode = node
        if (before_remainder != remainder) {
          location = "R"
          println("INFO::Move to the right node......")
          before_remainder = remainder
          depthNum = depthNum * 2 //깊이가 깊어질수록 노드 수는 두배가 되니까

          searchLeafNode(node.asInstanceOf[InternalNode].right)
        }
        else {
          location = "L"
          println("INFO::Move to the left node......")
          before_remainder = remainder
          depthNum = depthNum * 2 //깊이가 깊어질수록 노드 수는 두배가 되니까
          searchLeafNode(node.asInstanceOf[InternalNode].left)
        }

      case _: LeafNode =>
        println("INFO::Search Complete....!!! ")
        //printNode.printnode(inode)
        generateNewNode(node, inode.asInstanceOf[InternalNode].split_dim)
    }
  }


  //*leaf node가 존재하는 경우에 새로운 노드를 만드는 코드*/
  def generateNewNode(node:Node, splitValue:Int) = {
    val lnode=node.asInstanceOf[LeafNode]
    val min=lnode.mbr(splitValue).min
    val max=lnode.mbr(splitValue).max
    val quantDataRDD = lnode.value
    //quantDataRDD.map(x=>x.toList).coalesce(1).saveAsTextFile("file:////home/etri/code/log/quant_data/")

    println("\nINFO::Data reduce by key.....")
    quantDataRDD.cache()
    val quantDataResult = quantDataRDD.flatMap(x => x).reduceByKey(_ + _)//.sortByKey()

    val (split_dim,split_quantvalue) = Quantization.calVariance(quantDataResult,quantDataRDD.count().toInt,lnode)

    val (lNodeData, rNodeData) =kdtree.divideQuantizationRDD(quantDataRDD,split_dim,split_quantvalue) //leaf 노드의 데이터를 새로운 노드와 반나눠가짐

    quantDataRDD.unpersist()
    val leftNode = generateLeafNode("L", lNodeData, split_dim,split_quantvalue,lnode) //node가 가지고있던 데이털르 분할 받을 자식노드 2개를 생성
    val rightNode = generateLeafNode("R", rNodeData, split_dim,split_quantvalue,lnode)

    main.HDI.kdtree_leafnode_number = main.HDI.kdtree_leafnode_number + 1
    println("\nINFO::Tree leftnode +1 total leaftnode :"+ main.HDI.kdtree_leafnode_number )

    //inode가 현재로는 node의 부모노드이니까
    println("\nINFO::Convert LeafNode to InternalNode......")

    val queryRange = new queryRange

    val realSplitValue=(split_quantvalue+1) * main.HDI.voc

    if (location == "L") {inode.asInstanceOf[InternalNode].left = InternalNode(min,max,split_dim,realSplitValue, queryRange, leftNode, rightNode) //inode는 분활노드의 부모이고 그 부모의 자식이 지금 분할노드로 확인되어서 부모의 자식이 있던 위치를 location을 가지고 알아내서
    //printNode.printnode(inode.asInstanceOf[InternalNode].left)
    }
    //그 방향에 새로운 internalnode를 생성하고 이전에 새로 계산한 분산값들을 입력함
    else {
      inode.asInstanceOf[InternalNode].right = InternalNode(min, max, split_dim, realSplitValue, queryRange, leftNode, rightNode) //방향이 다르면 다른 방향에 삽입
      //printNode.printnode(inode.asInstanceOf[InternalNode].right)
    }
  }
}
/*노드 생성관련 코드 끝*/
