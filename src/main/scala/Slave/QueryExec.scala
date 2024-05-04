package Slave

import java.io.{BufferedReader, FileReader}

import org.apache.spark.rdd.RDD
import java.util

import scala.io.Source
import scala.util.control.Breaks._
import Master._
import scala.collection.mutable.Map


object QueryExec extends Serializable {
  def Query(RDDRootNode : RDD[(InternalNode, Int)],Query: (String, Int), PartitionNumber : List[Int], Arrayrefe :Array[(Array[ReferenceINFO],Int)], idx : Int, Path : String): Unit ={

    var querystarttime = System.currentTimeMillis()
    var query = Query
    var querypartition = PartitionNumber

    var Querystart = System.currentTimeMillis()

    // (RootNode , reference정보 , query , range , query처리할 영역 , 파티션번호)
    var QueryResult =  RDDRootNode.map({
      x => val Query = new Query(x._1, Arrayrefe, query._1, query._2, querypartition, x._2,Path)
        Query.ArrayResultSet
    }).reduce(_++_)

    var compareend = System.currentTimeMillis()

   var CandidateResult =  RDDRootNode.map({
      x => val Query = new Query(x._1, Arrayrefe, query._1, query._2, querypartition, x._2, Path)
        Query.SortedPoint
    }).reduce(_++_)

    /*    println("---------------------------Query Range(350)에 포함된 Data 결과------------------------------------")
        for(i<-0 until QueryResult.length)
        {
          println("Range("+Query._2+")에 포함된 "+i+1+"번째 Data : \t"+"Query : "+ QueryResult(i)._1 + "거리 : "+QueryResult(i)._2)
        }*/
    // println("Query total time :"+(compareend-Querystart)/1000.0)
    println(idx+1+"\t"+Query._2+"\t"+CandidateResult.length+"\t"+(compareend-querystarttime)/1000.0+"\t"+QueryResult.length)
  }
  def KNNQuery(RDDRootNode : RDD[(InternalNode, Int)], Arrayrefe :Array[(Array[ReferenceINFO],Int)], Query : String, MapParttion : Map[Int,Int], InitRange : Double, K : Int, Path : String):(Array[(String,Double)],Int,Int,Double)={
    var querystarttime = System.currentTimeMillis()
    var query = Query

    //println("MapParttion INFO : "+ MapParttion)
    var QueryResult = RDDRootNode.map({
      x=> var y = new KNNQuery(x._1,x._2,Arrayrefe,query,MapParttion,InitRange,Path,K)
        y.ArrayResultSet
    }).reduce{(a,b) =>
      val v1 = a._1 ++ b._1
      val v2 = Math.max(a._2,b._2)
      val v3 = a._3 + b._3
      val v4 = Math.max(a._4,b._4)
      (v1, v2, v3, v4)
    }

    var SortedQueryResult = QueryResult._1.sortBy(x=>x._2)


    var compareend = System.currentTimeMillis()
    /*    println("---------------------------Query KNN("+K+") 에 포함된 Data 결과------------------------------------")
        for(i<-0 until K)
          println((i+1)+"번째"+SortedQueryResult(i))*/

    println((compareend-querystarttime)/1000.0+"  "+QueryResult._3)

    //( CandidateResult , QueryResult._2 )
    (SortedQueryResult,QueryResult._2,QueryResult._3,QueryResult._4)
  }
  def ANNQuery(RDDRootNode : RDD[(InternalNode, Int)], Arrayrefe :Array[(Array[ReferenceINFO],Int)], Query : String, MapParttion : Map[Int,Int], InitRange : Double, K : Int, Path : String):(Array[(String,Double)],Double,Int)={
    var querystarttime = System.currentTimeMillis()
    var query = Query

    //println("MapParttion INFO : "+ MapParttion)
    var QueryResult = RDDRootNode.map({
      x=> var y = new ANNQuery(x._1,x._2,Arrayrefe,query,MapParttion,InitRange,Path,K)
        y.ArrayResultSet
    }).reduce{(a,b) =>
      val v1 = a._1 ++ b._1
      val v2 = Math.max(a._2,b._2)
      val v3 = a._3 + b._3
      (v1, v2, v3)
    }
    // v1 : 찾은 갯수 , v2 : 최종 range, v3 : 후보군
    var SortedQueryResult = QueryResult._1.sortBy(x=>x._2)


    var compareend = System.currentTimeMillis()
    /*    println("---------------------------Query KNN("+K+") 에 포함된 Data 결과------------------------------------")
        for(i<-0 until K)
          println((i+1)+"번째"+SortedQueryResult(i))*/

    println((compareend-querystarttime)/1000.0+"  "+QueryResult._1.size+"  "+QueryResult._2)

    //( CandidateResult , QueryResult._2 )
    (SortedQueryResult,QueryResult._2,QueryResult._3)
  }
}
class ANNQuery(RootNode: InternalNode, partition: Int, ReferencepointArr: Array[(Array[ReferenceINFO],Int)], querydata:String, MapPartition: Map[Int,Int], InitRange: Double, Path : String, k:Int){
  var K = Math.ceil(k/8.0)//Math.ceil(k/4.0) // 해당 파티션마다 다른 k개 비교 : MapPartition.get(partition).head  , 모든 파티션 k개 비교 : k
  var range = InitRange
  var value = 0
  for(i<-0 until 128) value+= 255*255 // 128차원
 // for(i<-0 until 16) value+= 255*255// 16차원
  var value2 = (math.sqrt(value) * 0.01).toInt

  var Key_start = 0.0
  var Key_end = 0.0
  var RealdataPointer = new util.ArrayList[Int]
  var StringData = querydata.split(" ")
  var QueryArray = new Array[Double](StringData.size)
  var SortedPoint: Array[Int] = null
  var Referencepoint:(Array[ReferenceINFO],Int) = null

  var ResultMap = scala.collection.mutable.Map(" "-> Double.MaxValue)
  var SortedMap:Seq[(String,Double)] = null
  var ResultDis = Double.MaxValue

  //var count = 0 // dpo 변경하기위해 사용 _ k개를 찾기위해 크기 증가를 몇번 하였는가.
  for(i<-0 until ReferencepointArr.length){
    if(ReferencepointArr(i)._2 == partition) Referencepoint = ReferencepointArr(i)
  }

  for (i <- 0 until StringData.size) QueryArray(i) = StringData(i).toDouble
  var count = 0
  var find_count = 0
  var before_count = 0
  var dpo = Double.MaxValue
  var before_dpo = 0.0
  var MaxDist = Double.MaxValue
  var MaxDist_check = true
  val th = 2.5 // 임계값

  var check = true
  while(check && K>0){
    var StartTime  = System.currentTimeMillis()
    range = range+value2


    for (i <- 0 until Referencepoint._1.length) {
      //Qeury와 Reference 거리계산
      var dis = new distance(Referencepoint._1(i).data, QueryArray, QueryArray.size).result
      //println(dis)
      // Query의 반경과 Reference pointer의 반경이 겹친다면
      if (dis <= Referencepoint._1(i).radius + range) {
        // Reference pointer의 중심이 지나는 경우
        if (range >= dis)
          Key_start = 0 + ((i) * new IDisConstantC(StringData.size).constantC)
        // 중심을 안지나는 경우
        else
          Key_start = (dis - range) + ((i) * new IDisConstantC(StringData.size).constantC)
        // Query영역이 Reference영역 안에 있는 경우
        if ((range + dis) <= Referencepoint._1(i).radius){
          if(Referencepoint._1(i).Key_End == 0.0){
            Key_end = dis  + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_Start = Key_start - ((i) * new IDisConstantC(StringData.size).constantC)

            Key_start = Key_end
            Key_end = range + dis + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_End = Key_end - ((i) * new IDisConstantC(StringData.size).constantC)
          }
          else{
            Key_end = Referencepoint._1(i).Key_Start + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_Start = Key_start - ((i) * new IDisConstantC(StringData.size).constantC)

            Key_start = Referencepoint._1(i).Key_End  + ((i) * new IDisConstantC(StringData.size).constantC)
            Key_end = range + dis + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_End = Key_end - ((i) * new IDisConstantC(StringData.size).constantC)
          }
        }
        // 아닌 경우
        else {
          Key_end = Referencepoint._1(i).Key_Start + ((i) * new IDisConstantC(StringData.size).constantC)
          new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
          Referencepoint._1(i).Key_Start = Key_start - ((i) * new IDisConstantC(StringData.size).constantC)
          Referencepoint._1(i).Key_End = Key_end - ((i) * new IDisConstantC(StringData.size).constantC)
        }
        //  println("Reference point:" + i + " Key_Start: " + Key_start + "Key_end: " + Key_end)

      }
    }
    //println(" Key_Start: " +Referencepoint._1(0).Key_Start+ "Key_end: " + Referencepoint._1(0).Key_End)
    //var Set = new util.Set[Int]
    SortedPoint = new Array[Int](RealdataPointer.size())
   // println("B+tree에서 나온 후보군 갯수" + RealdataPointer.size())
    var candinumber = RealdataPointer.size()
    for (i <- 0 until RealdataPointer.size()) {
      SortedPoint(i) = RealdataPointer.get(i)
    }
    //SortedPoint는 후보군 -> ResultMap도 같지 ..?? -> stop 하는 지점은...
    RealdataPointer.clear()
    java.util.Arrays.sort(SortedPoint)
    //print(SortedPoint.length+" ")

    new TxtCompare(SortedPoint,ResultMap,querydata,range,true,Path)
    // 포인터 값을 이용해 realdata를 찾고 거리 계산후 ResultMap에 저장
    //println("ResultMap: "+ResultMap.size +", "+K)
    if(ResultMap.size > 0){
      find_count = 0
      SortedMap = ResultMap.toSeq.sortBy(x=>x._2)
      //  println("Sorted: "+SortedMap.size)
      for(i<-0 until ResultMap.size){
        ResultDis = SortedMap(i)._2
        if(range > ResultDis){
          find_count+=1
        }
      }


      count = find_count
      println("Partition번호 : "+partition+", 찾은 갯수 : "+count+", Range : "+range)

    }

    var extra_count = K - count // 남은 k개

    if(extra_count < K){
      //println("current range : "+range)
      //before_dpo = dpo
      dpo = range / count
      if(MaxDist_check == true){
        MaxDist = range + (dpo*extra_count)
        MaxDist_check = false
      }
      if(before_count != find_count) before_dpo = dpo
      before_count = find_count

      println("Partition번호 : "+partition+", MaxDist : "+MaxDist+", dpo : "+dpo+", before_dpo : "+before_dpo)
      // dpo 계산
      if(dpo > before_dpo) println("Catch !! Partition :"+partition+", dpo :"+dpo+", before_dpo : "+before_dpo)
      if(count <= K ) check = true // MaxDist > range && dpo <= before_dpo *th && 
      else check = false
    }
    var endTime  = System.currentTimeMillis()
    println("Partition번호 : "+partition+", current range : "+range+", B+tree에서 나온 후보군 갯수 : "+candinumber+", Time : "+(endTime-StartTime)/1000.0)
    /* println("ResultMap.size"+ResultMap.size)

     println("Range : "+range+" 나온 갯수 : "+ResultMap.size+" 현재 K번째("+K+") 거리"+ResultDis+" Partition :"+partition)*/
  }
  //println("count "+ count)
  //println("total candidate = "+SortedMap.size+", Partition number : "+partition)
  var ArrayResultSet = (new Array[(String,Double)](count),range,ResultMap.size)
  println(count +", "+ArrayResultSet._1.size +", "+SortedMap.size)
  for (i <- 0 until count) {
    ArrayResultSet._1(i) = SortedMap(i)
  }

}

class KNNQuery(RootNode: InternalNode, partition: Int, ReferencepointArr: Array[(Array[ReferenceINFO],Int)], querydata:String, MapPartition: Map[Int,Int], InitRange: Double, Path : String, k:Int){
  var K = k // 해당 파티션마다 다른 k개 비교 : MapPartition.get(partition).head  , 모든 파티션 k개 비교 : k
  var range = InitRange
  var value = 0
  for(i<-0 until 128) value+= 255*255 // 128차원
 // for(i<-0 until 16) value+= 255*255 // 16차원
  var value2 = (math.sqrt(value) * 0.01).toInt

  var Key_start = 0.0
  var Key_end = 0.0
  var RealdataPointer = new util.ArrayList[Int]
  var StringData = querydata.split(" ")
  var QueryArray = new Array[Double](StringData.size)
  var SortedPoint: Array[Int] = null
  var Referencepoint:(Array[ReferenceINFO],Int) = null

  var ResultMap = scala.collection.mutable.Map(" "-> Double.MaxValue)
  var SortedMap:Seq[(String,Double)] = null
  var ResultDis = Double.MaxValue

  var count = 0 // dpo 변경하기위해 사용 _ k개를 찾기위해 크기 증가를 몇번 하였는가.
  for(i<-0 until ReferencepointArr.length){
    if(ReferencepointArr(i)._2 == partition) Referencepoint = ReferencepointArr(i)
  }

  for (i <- 0 until StringData.size) QueryArray(i) = StringData(i).toDouble

  while(ResultDis > range && range < math.sqrt(value).toInt && K>0){
    range = range+value2
    count += 1
    for (i <- 0 until Referencepoint._1.length) {
      //Qeury와 Reference 거리계산
      var dis = new distance(Referencepoint._1(i).data, QueryArray, QueryArray.size).result
      //println(dis)
      // Query의 반경과 Reference pointer의 반경이 겹친다면
      if (dis <= Referencepoint._1(i).radius + range) {
        // Reference pointer의 중심이 지나는 경우
        if (range >= dis)
          Key_start = 0 + ((i) * new IDisConstantC(StringData.size).constantC)
        // 중심을 안지나는 경우
        else
          Key_start = (dis - range) + ((i) * new IDisConstantC(StringData.size).constantC)
        // Query영역이 Reference영역 안에 있는 경우
        if ((range + dis) <= Referencepoint._1(i).radius){
          if(Referencepoint._1(i).Key_End == 0.0){
            Key_end = dis  + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_Start = Key_start - ((i) * new IDisConstantC(StringData.size).constantC)

            Key_start = Key_end
            Key_end = range + dis + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_End = Key_end - ((i) * new IDisConstantC(StringData.size).constantC)
          }
          else{
            Key_end = Referencepoint._1(i).Key_Start + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_Start = Key_start - ((i) * new IDisConstantC(StringData.size).constantC)

            Key_start = Referencepoint._1(i).Key_End  + ((i) * new IDisConstantC(StringData.size).constantC)
            Key_end = range + dis + ((i) * new IDisConstantC(StringData.size).constantC)
            new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
            Referencepoint._1(i).Key_End = Key_end - ((i) * new IDisConstantC(StringData.size).constantC)
          }
        }
        // 아닌 경우
        else {
          Key_end = Referencepoint._1(i).Key_Start + ((i) * new IDisConstantC(StringData.size).constantC)
          new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
          Referencepoint._1(i).Key_Start = Key_start - ((i) * new IDisConstantC(StringData.size).constantC)
          Referencepoint._1(i).Key_End = Key_end - ((i) * new IDisConstantC(StringData.size).constantC)
        }
        //  println("Reference point:" + i + " Key_Start: " + Key_start + "Key_end: " + Key_end)

      }
    }
    //println(" Key_Start: " +Referencepoint._1(0).Key_Start+ "Key_end: " + Referencepoint._1(0).Key_End)
    //var Set = new util.Set[Int]
    SortedPoint = new Array[Int](RealdataPointer.size())
    //println("B+tree에서 나온 후보군 갯수" + RealdataPointer.size())
    for (i <- 0 until RealdataPointer.size()) {
      SortedPoint(i) = RealdataPointer.get(i)
    }
    //SortedPoint는 후보군 -> ResultMap도 같지 ..?? -> stop 하는 지점은...
    RealdataPointer.clear()
    java.util.Arrays.sort(SortedPoint)
    //print(SortedPoint.length+" ")

    new TxtCompare(SortedPoint,ResultMap,querydata,range,true,Path)

    //println("ResultMap: "+ResultMap.size +", "+K)
    if(ResultMap.size>K && K > 0){
      SortedMap = ResultMap.toSeq.sortBy(x=>x._2)
      //  println("Sorted: "+SortedMap.size)
      ResultDis = SortedMap(K-1)._2
      println(K+"번째 ResultDis"+ResultDis+", Range : "+range)
    }

   /* println("ResultMap.size"+ResultMap.size)

    println("Range : "+range+" 나온 갯수 : "+ResultMap.size+" 현재 K번째("+K+") 거리"+ResultDis+" Partition :"+partition)*/
  }
  //println("count "+ count)
  //println("total candidate = "+SortedMap.size+", Partition number : "+partition)
  var ArrayResultSet = (new Array[(String,Double)](K),count,ResultMap.size, range)

  for (i <- 0 until K) {
    ArrayResultSet._1(i) = SortedMap(i)
  }

}
class Query(RootNode: InternalNode, ReferencepointArr: Array[(Array[ReferenceINFO],Int)], querydata: String, range: Double, querypartition: List[Int],
            partition: Int,Path : String) extends Serializable {
  //println("Inter Query Start")

  var Key_start = 0.0
  var Key_end = 0.0
  var RealdataPointer = new util.ArrayList[Int]
  var StringData = querydata.split(" ")
  var QueryArray = new Array[Double](StringData.size)
  var SortedPoint = new Array[Int](0)
  var Referencepoint:(Array[ReferenceINFO],Int) = null
  //var ResultSet = new util.ArrayList[(String,Double)]
  var ResultMap = scala.collection.mutable.Map(""-> 0.0)
  var ArrayResultSet = new Array[(String,Double)](0)
  var ResultNumber = 0
  if(querypartition.contains(partition)){
    for(i<-0 until ReferencepointArr.length){
      if(ReferencepointArr(i)._2 == partition) Referencepoint = ReferencepointArr(i)
    }

    for (i <- 0 until StringData.size) QueryArray(i) = StringData(i).toDouble
    // Reference 갯수만큼 확인
    for (i <- 0 until Referencepoint._1.length) {
      //Qeury와 Reference 거리계산
      var dis = new distance(Referencepoint._1(i).data, QueryArray, QueryArray.size).result

      // Query의 반경과 Reference pointer의 반경이 겹친다면
      if (dis <= Referencepoint._1(i).radius + range) {
        // Reference pointer의 중심이 지나는 경우
        if (range >= dis)
          Key_start = 0 + ((i) * new IDisConstantC(StringData.size).constantC)
        // 중심을 안지나는 경우
        else
          Key_start = (dis - range) + ((i) * new IDisConstantC(StringData.size).constantC)
        // Query영역이 Reference영역 안에 있는 경우
        if ((range + dis) <= Referencepoint._1(i).radius)
          Key_end = range + dis + ((i) * new IDisConstantC(StringData.size).constantC)
        // 아닌 경우
        else
          Key_end = Referencepoint._1(i).radius + ((i) * new IDisConstantC(StringData.size).constantC)

        //println("Reference point:" + i + " Key_Start: " + Key_start + "Key_end: " + Key_end)
        new find_and_print_range(RootNode, Key_start, Key_end, RealdataPointer)
      }
    }


    SortedPoint = new Array[Int](RealdataPointer.size())

    for (i <- 0 until RealdataPointer.size()) {
      SortedPoint(i) = RealdataPointer.get(i)
    }

    java.util.Arrays.sort(SortedPoint)
    // print(SortedPoint.length+" ")

   // var txtstarttime = System.currentTimeMillis()
    new TxtCompare(SortedPoint,ResultMap,querydata,range,false,Path)

    //  var txtendtime = System.currentTimeMillis()

   // println("asdasd : "+(txtendtime-txtstarttime)/1000.0+"  B+tree에서 나온 후보군 갯수" + RealdataPointer.size)
    RealdataPointer.clear()
    ResultNumber = ResultMap.size
    ArrayResultSet = new Array[(String,Double)](ResultMap.size)
    for (i <- 0 until ResultMap.size) {
      ArrayResultSet(i) = ResultMap.toSeq(i)
    }
  }

}


class TxtCompare(SortedPoint : Array[Int] ,ResultSet : scala.collection.mutable.Map[String,Double],querydata : String,range : Double, flag : Boolean, Path : String){
  var ArrayPointer:Array[Int] = null

  ArrayPointer = SortedPoint
//  var txtreadstart = System.currentTimeMillis()
  var br = new BufferedReader(new FileReader(Path))
 // var txtreadend = System.currentTimeMillis()
 // println("txt Read : "+(txtreadend-txtreadstart)/1000.0)

 // var txtstart = System.currentTimeMillis()
  for(i<-0 until ArrayPointer.length){
    var idx = 0
    if(i == 0) idx = ArrayPointer(0)
    else idx = ArrayPointer(i)-ArrayPointer(i-1)-1
    // idx 가 -1이 나오는 이유는 현재 Arraypointer와 직전 Arraypointer가 같아서이다. 왜 같은게 나오는지 이유는 찾지 못함..
    if(idx == -1 ) {}//println("i:"+i+" idx"+idx,", Arraypoint(i)"+ArrayPointer(i)+", Arraypoint(i-1)"+ArrayPointer(i-1))
    else{
      var line = br.lines().skip(idx).findFirst().get()
      var data = line.substring(0,line.indexOf("  "))
      //var data = line  // data에 파일 경로? 이름이 존재하지 않고 data값만 존재할 경우
      var Stringdata = data.split(" ")
      //var Stringdata = line.split(" ")
      var dataArr = new Array[Double](Stringdata.size)
      for(i<-0 until Stringdata.length) dataArr(i) = Stringdata(i).toDouble
      var compare = new compare(dataArr,querydata,range, flag)
      var temp = compare.Result
      var dis = compare.dis
      //println(dis)
      if(temp.equals("")==false) ResultSet += (temp -> dis)
    }

  }
 // var txtend = System.currentTimeMillis()
//  println("txt compare\t"+(txtend-txtstart)/1000.0)
}

class find_and_print_range(RootNode: InternalNode, key_start: Double, key_end: Double, RealdataPointer: util.ArrayList[Int]) extends Serializable  {
  var tempNode = RootNode
  var find_LeafNode: LeafNode = null

  var i = 0
  breakable {
    while (tempNode.isInstanceOf[InternalNode]) {
      //println(i+" "+tempNode.header.numOfdata+" "+tempNode.entry.data(i))
      if (tempNode.entry.data(i).asInstanceOf[Double] >= key_start) {

        if (tempNode.entry.pointers(i).isInstanceOf[LeafNode]) {
          find_LeafNode = tempNode.entry.pointers(i).asInstanceOf[LeafNode]
          break
        }
        else {
          tempNode = tempNode.entry.pointers(i).asInstanceOf[InternalNode]
          i = 0
        }
      }
      else {
        if (i >= tempNode.header.numOfdata - 1) {
          if (tempNode.entry.pointers(i + 1).isInstanceOf[LeafNode]) {
            find_LeafNode = tempNode.entry.pointers(i + 1).asInstanceOf[LeafNode]
            break
          }
          else {
            tempNode = tempNode.entry.pointers(i + 1).asInstanceOf[InternalNode]
            i = 0
          }
        }
        else i += 1
      }
    }
  }

  //LeafNode Data값 탐색후  pointer 값 저장
  breakable {
    while (find_LeafNode != null) {
      for (i <- 0 until find_LeafNode.header.numOfdata) {

        if (key_end < find_LeafNode.entry.data(i).asInstanceOf[Double]) break

        if (key_start <= find_LeafNode.entry.data(i).asInstanceOf[Double]) {
          RealdataPointer.add(find_LeafNode.entry.pointers(i).asInstanceOf[Int])
        }
        if (i == (find_LeafNode.header.numOfdata - 1))
          find_LeafNode = find_LeafNode.header.NextNode
      }
    }
  }

}


class compare(realdata: Array[Double], querydata: String, range: Double, flag : Boolean) {
  //println(realdata)
  var Result = ""

  var StringData = querydata.split(" ")
  var QueryArray = new Array[Double](StringData.size)

  for (i <- 0 until StringData.size) QueryArray(i) = StringData(i).toDouble
  var dis = new distance(realdata, QueryArray, QueryArray.size).result

  if (dis <= range) {
    Result = realdata.mkString(" ")
  }
  if(flag == true) Result = realdata.mkString(" ")
}
