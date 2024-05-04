package Master

import java.util

import Master.main.HDI

import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import Slave._

import scala.collection.mutable.Map

object OnlySlave {
  //broadcast 변수 설정
  var broadKd : Broadcast[Master.ROOTINFO.type] = null;
  var broadHdi :Broadcast[Master.HDIClass] = null;
  val HDI = new HDIClass

  def mapfunc(Partition: Int, iter: Iterator[((Array[Double],Array[String]), Long)]): Iterator[((Array[Double], Int), Int,Array[String])] = {
    iter.map(x => ((x._1._1, Partition.abs), x._2.toInt, x._1._2)).toIterator
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("ETRI")
      .setMaster("spark://203.255.92.39:7077")

    conf.set("spark.driver.maxResultSize","3g")

    val sc         = SparkContext.getOrCreate(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    HDI.file_name    = args(0)
    HDI.split_number = args(1).toInt




    var RealDataRDD = sc.textFile("file:///" + HDI.file_name).map(
          x => (x.split(" ").take(HDI.featureSize).map(_.toDouble), x.split(" ").takeRight(1))).zipWithIndex().repartition(4)
    var startmaster = System.currentTimeMillis()
    print("INFO::파티션 결과 : ")
    println(RealDataRDD.mapPartitions(iter => Iterator(iter.size), true).collect().toList)
    var result = RealDataRDD.mapPartitionsWithIndex(mapfunc)

    //Slave index 구축
    val startSlave = System.currentTimeMillis()
    var SlaveINFO  = Slave.SlaveMain.SlaveMain(result)
    val end        = System.currentTimeMillis()

    //println("total time (index) : " + (end - startmaster) / 1000.0)
    println("Index Complete : " + (end - startSlave) / 1000.0)

    //Quuery 처리

    var ArrayQuery       = new util.ArrayList[String]()
    var LimitQueryNumber = new Array[Int](2)
    var LimitRange       = new Array[Int](10)




    //    LimitRange(0) = 28
    //    LimitRange(1) = 28
    //    LimitRange(2) = 28
    //    LimitRange(3) = 28
    //    LimitRange(4) = 28
    //    LimitRange(5) = 28
    //    LimitRange(6) = 28
    //    LimitRange(7) = 28
    //    LimitRange(8) = 28
    //    LimitRange(9) = 28


    LimitRange(0) = 80
    LimitRange(1) = 90
    LimitRange(2) = 100
    LimitRange(3) = 109
    LimitRange(4) = 110
    LimitRange(5) = 120
    LimitQueryNumber(0) = 30
    LimitQueryNumber(1) = 100


    // args(2)
    val bufferdSource = Source.fromFile(args(2))
    //val bufferdSource = Source.fromFile("/root/ETRI/yeon/Query100_100ran.txt")
    for (line <- bufferdSource.getLines()) {

      ArrayQuery.add(line)
    }
    bufferdSource.close()

/*    //10개의 Query * Range 5개 실행 = 50개의 결과값
    println("Query\tRange\tcandidate\tTotaltime")
    for(i<-0 until 6){
      for(j<-0 until LimitQueryNumber(0)) {
        val queryClass = new kdTreeRangeQuery()

        //var querydata = ArrayQuery.get(j).split(" ").map(_.toInt)
        //val resultPartitionNumbers= queryClass.retrivalKdtree(ROOTINFO.root,LimitRange(i)/10,querydata)
        val resultPartitionNumbers = List(1,2,3,4,5,6,7,8)
        QueryExec.Query(SlaveINFO._1,(ArrayQuery.get(j),LimitRange(i)),resultPartitionNumbers.toList,SlaveINFO._2,j,HDI.file_name)
      }
    }*/

    // 50개의 Query * Range:50 실행 = 90개의 결과값
    /*
        println("----------------100 Query--------------------------------")
        println("\t\t\tQuery\tRange\tcandidate\tTotaltime")
        for(rg<-0 until 2){
          for (j <- 0 until 100) {
            val rangeQueryClass = new kdTreeRangeQuery()
            var querydata = ArrayQuery.get(j).split(" ").map(_.toInt)
           val resultPartitionNumbers = rangeQueryClass.retrivalKdtree(ROOTINFO.root, LimitRange(rg), querydata)
          // val resultPartitionNumbers = List(1,2,3,4,5,6,7,8)
            //print(resultPartitionNumbers.toList)
            // kdtree.printArray(resultPartitionNumbers.toArray)
            // B+tree, (쿼리Data , Range), 파티션번호, 레퍼런스정보
            QueryExec.Query(SlaveINFO._1, (ArrayQuery.get(j), LimitRange(rg)), resultPartitionNumbers.toList, SlaveINFO._2, j,HDI.file_name)
          }
        }

    */

/*
    // KNN Query - 정보과학회 knn 구현 기존 IDistance 사용. dpo변경 전
    for(i<-1 until 2){
      println("---------------100 Query(K:100)--------------------------------")
      println("Query\tTotaltime\tcandidate")
      // val kResult = Map[Int,Int]()  // 무의미함.. query에 따른 파티션마다 k개를 다르게 하기위해 썻던건데 현재 여기서는 필요없음.
      for (j <-0 until 100) {
        print(j+1+"  ")
        val knnQueryClass = new kdTreeKnnQuery()
        var querydata = ArrayQuery.get(j).split(" ").map(x=>x.toDouble.abs.toInt)
        val k = 10
        //val (kResult, queryRange, qPartition) = knnQueryClass.knnQuery(k, querydata)
        val kResult:Map[Int,Int] = Map(0->k)


        //(RootNode: InternalNode, partition: Int, ReferencepointArr: Array[(Array[ReferenceINFO],Int)], querydata:String, MapPartition: Map[Int,Int], InitRange: Int)
        val queryResult = QueryExec.KNNQuery(SlaveINFO._1,SlaveINFO._2,ArrayQuery.get(j),kResult,0,k,HDI.file_name) // 기존기법 range: 28 // 구현기법 : queryRange  - dpo 변경전

        //아래는 슬레이브 결과 받고나서.
        // val avgDist = queryResult._1.map(x => x._2).sum / queryResult._1.size
        //  val node = ROOTINFO.partitionDim(qPartition).dpo = (1 / avgDist) * queryResult._1.size


      }
    }
*/
    // ANN - AP-KNN (2) 방법
    for(i<-1 until 2){
      println("---------------100 Query(K:100)--------------------------------")
      println("Query\tTotaltime\tcandidate")
      // val kResult = Map[Int,Int]()  // 무의미함.. query에 따른 파티션마다 k개를 다르게 하기위해 썻던건데 현재 여기서는 필요없음.
      for (j <-2 until 3) {
        print(j+1+"  ")
        val knnQueryClass = new kdTreeKnnQuery()
        var querydata = ArrayQuery.get(j).split(" ").map(x=>x.toDouble.abs.toInt)
        val k = 10
        //val (kResult, queryRange, qPartition) = knnQueryClass.knnQuery(k, querydata)
        val kResult:Map[Int,Int] = Map(0->k)


        //(RootNode: InternalNode, partition: Int, ReferencepointArr: Array[(Array[ReferenceINFO],Int)], querydata:String, MapPartition: Map[Int,Int], InitRange: Int)
        val queryResult = QueryExec.ANNQuery(SlaveINFO._1,SlaveINFO._2,ArrayQuery.get(j),kResult,0,k,HDI.file_name) // 기존기법 range: 28 // 구현기법 : queryRange  - dpo 변경전

        //아래는 슬레이브 결과 받고나서.
        // val avgDist = queryResult._1.map(x => x._2).sum / queryResult._1.size
        //  val node = ROOTINFO.partitionDim(qPartition).dpo = (1 / avgDist) * queryResult._1.size


      }
    }

    /*    // KNN Query - 콘텐츠학회 knn 구현에서의 dpo 비교 / dpo변경 vs dpo수렴
        for(i<-1 until 2){
          var count = 0
          val kResult = Map[Int,Int]()  // 무의미함.. query에 따른 파티션마다 k개를 다르게 하기위해 썻던건데 현재 여기서는 필요없음.
          kResult += (0->0)
          var queryRange = 28.0
          var threshold = 0
          println("---------------100 Query(K:100)--------------------------------")
          println("Query\tTotaltime\tcandidate")
          for (j <-0 until 100) {
            print(j+1+" "+queryRange+"  ")
            val k = 10

            //(RootNode: InternalNode, partition: Int, ReferencepointArr: Array[(Array[ReferenceINFO],Int)], querydata:String, MapPartition: Map[Int,Int], InitRange: Int)
            val queryResult = QueryExec.KNNQuery(SlaveINFO._1,SlaveINFO._2,ArrayQuery.get(j),kResult,queryRange,k,HDI.file_name) // 기존기법 range: 28 // 구현기법 : queryRange  - dpo 변경전
            //print(", "+queryResult._3)
            count = queryResult._2
            if(j==0) threshold = queryResult._3
            if(count == 1 && threshold > queryResult._3) queryRange *= 0.995
            else if(count > 1) queryRange = queryRange * (1.0 + count*0.01)
          }
        }*/

  }
}

//result.map(x=>x._1._1.toList).saveAsTextFile("file:////home/etri/code/log/partition_result")

// val querydata = Source.fromFile(HDI.query_file_name).getLines.mkString.split(" ").map(_.toInt)

//queryClass.printArray(resultPartitionNumbers)

//Slave.SlaveMain.SlaveMain(resultValue)

// var a = RealDataRDD.partitionBy(2)
/*    var b= RealDataRDD.repartition(16)
    var testb = b.mapPartitionsWithIndex(mapfunc)
    println("testb PartitionNumber "+ testb.groupBy(_._1._2).count())*/
//var partitionresult = result.mapPartitionsWithIndex(mapfunc)
//println("partition Number : "+partitionresult.getNumPartitions)
//println("Group by"+ partitionresult.groupBy(_._1._2).count())

//println("kREsult"+kResult)
// println("tempRAnge"+queryRange)

//QueryExec.KNNQuery(SlaveINFO._1,)

//   for(pNumber <- 0 until 4){
//     val node = ROOTINFO.partitionDim(pNumber)
//     println("노드"+pNumber+" dpo 변경전 "+node.asInstanceOf[LeafNode].dpo)
//    }

//    for(pNumber <- 0 until 4){
//     val node = ROOTINFO.partitionDim(pNumber)
//     println("노드"+pNumber+" dpo 변경후 "+node.asInstanceOf[LeafNode].dpo)
//   }
