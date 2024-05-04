package Slave
import java.util

import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.control.Breaks._
import org.apache.spark.{SparkConf, SparkContext}


// ((RootNode,Partition),(ReferenceINFO, Partition))
object SlaveMain extends Serializable {
  def  SlaveMain(PartitionRDD : RDD[((Array[Double],Int),Int,Array[String])]): (RDD[(InternalNode,Int)] , Array[(Array[ReferenceINFO],Int)]) ={

    val dimension = 128 // 128차원 , 16차원

    var start = System.currentTimeMillis()

    val RealDataRDD = PartitionRDD.map(x=>(x._1,x._2))

    var Arrayrefestart = System.currentTimeMillis()
    var Arrayrefe = new IDistance_ReferencePoint(RealDataRDD).referenceTestArray

    // reference와 realdata의 키값 구하기  ( keydata - (  파티션번호,value ) , Minidx - reference번호 , MIN - 거리 , 인덱스번호 )
    var IDistanceValue= RealDataRDD.map({
      x =>
        var RDDValue = new IDistance_CreateKey(Arrayrefe,x._1,dimension)
        (RDDValue.keyData, RDDValue.MINidx, RDDValue.MIN, x._2)
    })
    //println("referece point 생성 완료")
    IDistanceValue.persist()

    // reference pointer 들의 반지름 구하기 ( MIN 과 Minidx 이용 )
    for(j<-0 until Arrayrefe.length){
      println("refe total number : "+Arrayrefe(j)._1.length)
      var partvalue = IDistanceValue.filter(x=>x._1._1.equals(Arrayrefe(j)._2))
      partvalue.persist()
      for(i<-0 until Arrayrefe(j)._1.length){
        var Valuefilter = partvalue.filter({
          x=> x._2.equals(i)
        }).map(x=>x._3)
        if(Valuefilter.count() != 0) {
          Arrayrefe(j)._1(i).radius = Valuefilter.max()
          Arrayrefe(j)._1(i).Key_Start = Valuefilter.max()
        }
        partvalue.unpersist()
      }
    }

      /*  for(i<-0 until Arrayrefe.size){
          for(j<-0 until Arrayrefe(i)._1.length-1){
            for(k<-j+1 until Arrayrefe(i)._1.length){
              var a = new distance(Arrayrefe(i)._1(j).data,Arrayrefe(i)._1(k).data,dimension).result
              print(a+", "+Arrayrefe(i)._1(j).radius+", "+Arrayrefe(i)._1(k).radius+" => ")
              if(a< Arrayrefe(i)._1(j).radius + Arrayrefe(i)._1(k).radius) println(j+"와 "+k+"는 겹쳐있다")
              else println(j+"와 "+k+"는 떨어져있다")
            }
          }
          println("")
        }*/
    var Btreeend = System.currentTimeMillis()
    //println("key Value generattion time :"+(Btreeend-start)/1000.0)
    // 같은 파티션끼리 그룹화 ( ex) 1,2가 있으면 1인파티션끼리 묶임 )

    var GroupRDD =IDistanceValue.groupBy(x=>x._1._1).map(x=>x._2.toArray)
    println(GroupRDD.count())

    println("---------------------IDISTANCE_KEY 생성 완료---------------------")


    println("---------------------B+Tree Start---------------------")

    //각 x의 공간마다 파티션번호를 부여해줘야된느데 잘안된다
    var RDDRootNode = GroupRDD.map({
      x => var Root = new Tree(x)
        (Root.RootNode, Root.PartitionNumber)
    })
    // println(RDDRootNode.first()._1)

    RDDRootNode.persist()
    //println(RDDRootNode.count()+" , "+RDDRootNode.getNumPartitions)
    println("---------------------B+Tree Complete---------------------")
    println("B+Tree 갯수(파티션당 1개)  :"+ RDDRootNode.count())

    /*var PrintNode = RDDRootNode.collect()
    for(i<-0 until PrintNode.length){
      println(i+1+" 번쨰 Partition의 RootNode : "+PrintNode(0)._1)
      var RootNode:InternalNode = PrintNode(i)._1
      var leafNode:LeafNode=null
      var printNode = RootNode
      var isleaf = false
      while(isleaf==false){
        if(printNode.entry.pointers(0).isInstanceOf[InternalNode]) printNode = printNode.entry.pointers(0).asInstanceOf[InternalNode]
        else{
          isleaf=true
          leafNode = printNode.entry.pointers(0).asInstanceOf[LeafNode]
        }
      }
      for(j<-0 until leafNode.header.numOfdata){
        println("value:" + leafNode.entry.data(j) + ", pointer:" + leafNode.entry.pointers(j))
      }
    }*/

    (RDDRootNode,Arrayrefe)
  }
}
/*class SlaveMain(PartitionRDD : RDD[((Array[Double],Int),Int,Array[String])]) extends Serializable {

}*/




