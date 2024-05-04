package Slave

import java.util

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class IDistance_ReferencePoint2(dimension : Int, NumberofReference : Int, PartitionNumber : Int) extends Serializable{
  var Min = 0
  var Max = 255

  val ran = scala.util.Random
  var randomdata = new Array[Double](dimension)
  var referenceTestArray = new Array[Array[ReferenceINFO]](PartitionNumber)

  for(i<-0 until PartitionNumber){
    referenceTestArray(i) = new Array[ReferenceINFO](NumberofReference)
    for(j<-0 until NumberofReference){
      var referenceINFO = new ReferenceINFO(NumberofReference)
      for(k<-0 until dimension)  randomdata(k) = 255*ran.nextDouble()
      referenceINFO.data = randomdata
      referenceTestArray(i)(j) = referenceINFO
    }
  }
}

class ReferenceINFO(dimention : Int)  extends Serializable{
  var data = new Array[Double](dimention)
  var radius:Double = 0
  var Partition = 0
  var Key_Start = 0.0
  var Key_End = 0.0
}


class distance(dis1 : Array[Double], dis2 : Array[Double], dimention: Int){
  var sum = 0.0
  var result = 0.0

  for(i<-0 until dimention){
    sum += ( ( dis1(i) - dis2(i) ) * ( dis1(i) - dis2(i) ) )
  }
  result = Math.sqrt(sum)
}


class IDistance_CreateKey(Referencepoint : Array[(Array[ReferenceINFO],Int)], realdata : (Array[Double],Int),dimention:Int) extends Serializable{

  var keyData:(Int,Double)= (0,0.0)   // real Data key 저장 공간
  var StringKeyData =""
  var refe = Referencepoint

  var MIN = -1.0
  var MINidx = 0
  var number = 0

  for(i<-0 until Referencepoint.length){
    if(Referencepoint(i)._2 == realdata._2){
      for(k<-0 until Referencepoint(i)._1.size) {
        var dis = new distance(Referencepoint(i)._1(k).data, realdata._1, dimention).result
        if (dis < MIN || MIN == -1.0) {
          MIN = dis
          MINidx = k
          keyData = (realdata._2, (MIN + (k * new IDisConstantC(dimention).constantC)))
        }
      }
    }
  }
  StringKeyData = keyData.toString
}
class IDisConstantC(dimention:Int){
  var constantC = Math.sqrt(dimention)*255
}

class IDistance_ReferencePoint(RealdataRDD : RDD[((Array[Double],Int),Int)]) extends Serializable {

  var dimension = 128
  var selectdim = (dimension * 0.1).toInt
  val featureSize = 2
  var count = 1



  var GroupRDD = RealdataRDD.map(x=>x._1).groupBy(_._2).map(x=>x._2.toArray)
  GroupRDD.persist()

  val thresholdDataNum = (RealdataRDD.count())*0.005
  val beforeVar = 0

  var rpResRDD = GroupRDD.map(
    {x=> var Refe = new ReferencePoint(x,beforeVar,thresholdDataNum,dimension,selectdim)
      (Refe.rpRes,Refe.PartitionNumber)})

  var rpResArray  =rpResRDD.collect()
  var PartitionNumber = GroupRDD.count().toInt
  var refeaa = new Array[(Array[ReferenceINFO],Int)](PartitionNumber)
  var referenceTestArray = new Array[(Array[ReferenceINFO],Int)](PartitionNumber)
  for(i<-0 until PartitionNumber){
    referenceTestArray(i) = (new Array[ReferenceINFO](rpResArray(i)._1.length),rpResArray(i)._2)
    for(j<-0 until rpResArray(i)._1.length){
      var referenceINFO = new ReferenceINFO(dimension)
      referenceINFO.data = rpResArray(i)._1(j)
      referenceINFO.Partition = rpResArray(i)._2
      referenceTestArray(i)._1(j) = referenceINFO
    }
  }
}
class ReferencePoint(x: Array[(Array[Double],Int)],beforeVar: Double, thresholdDataNum : Double, dimension:Int, selectdim:Int) extends Serializable {

  val PartitionNumber = x(0)._2
  val LimitCount = 32
  val rpRes = new ArrayBuffer[Array[Double]]()
  val RealDataNumber = new util.ArrayList[(Array[(Array[Double],Int)],Int)]()
  BPSclusteringRdd(x, beforeVar, thresholdDataNum, dimension, selectdim,0)

  /*for(i<-0 until rpRes.size){
     for(j<-0 until rpRes(0).length)
         print(rpRes(i)(j)+" ")
    println("")
  }*/

  def BPSclusteringRdd(RealData: Array[(Array[Double], Int)], beforeVar: Double, thresholdDataNum: Double, dimension: Int, selectdim: Int, flag :Int): Unit = {
    val countRddRealDataNum = RealData.length
    RealDataNumber.add((RealData,countRddRealDataNum))

    while(RealDataNumber.size < 128){

      var divData = RealDataNumber.get(0)
      var removeidx = 0
      for(i<-1 until RealDataNumber.size()){
        if(RealDataNumber.get(i)._2 > divData._2) {
          divData = RealDataNumber.get(i)
          removeidx = i
        }
      }
      RealDataNumber.remove(removeidx)

      val calvarRes = calvar(divData._1, divData._2)

      val topDimArrcalvar = calvarRes._1.sortBy(_._2).reverse.take(selectdim)
      //분산값이 제일큰거 10% 선정
      val topDimArr = new Array[Int](selectdim)
      for (i <- 0 until selectdim) topDimArr(i) = topDimArrcalvar(i)._1

      val calDimLenRes = calDimLen(divData._1, topDimArr, divData._2)
      val splitDim = calDimLenRes._1.sortBy(_._2).reverse.take(1)
      // 그중 길이가 제일 큰거 선택


      val avgDimData = calDimLenRes._2

      var currentSplitDimVar = 0.0
      for (i <- 0 until selectdim) {
        if (topDimArrcalvar(i)._1 == splitDim(0)._1) currentSplitDimVar = topDimArrcalvar(i)._2

      }

      val AvgData = splitDim(0)._3
      if(flag == 0) avgDimData(splitDim(0)._1) = splitDim(0)._5
      else avgDimData(splitDim(0)._1) = splitDim(0)._4

      val splitRdd = splitRealRdd(divData._1, splitDim(0)._1, AvgData)
      val leftRdd = splitRdd._1
      val rightRdd = splitRdd._2

      val leftCount = leftRdd.length
      val rightCount = rightRdd.length

      if(leftCount == 0 || rightCount == 0){
        println(divData._1.length+"갯수 0존재"+leftCount+" "+rightCount)
      }
      RealDataNumber.add((leftRdd,leftRdd.length))
      RealDataNumber.add((rightRdd,rightRdd.length))
    }
    for(i<-0 until RealDataNumber.size()){
      var avgData = new Array[Double](dimension)
      for(j<-0 until dimension){
        var sum = 0
        for(k<-0 until RealDataNumber.get(i)._1.size){
          sum = sum + RealDataNumber.get(i)._1(k)._1(j).toInt
        }
        //println(j+"asdasfaf"+ RealDataNumber.get(i)._1.size)
        avgData(j) = (sum/RealDataNumber.get(i)._1.size).toInt
      }
      rpRes += avgData
    }
  }

  def clusteringRdd(RealData: Array[(Array[Double], Int)], beforeVar: Double, thresholdDataNum: Double, dimension: Int, selectdim: Int, flag :Int): Unit = {

    val countRddRealDataNum = RealData.length

    val calvarRes = calvar(RealData, countRddRealDataNum)

    val topDimArrcalvar = calvarRes._1.sortBy(_._2).reverse.take(selectdim)
    //분산값이 제일큰거 10% 선정
    val topDimArr = new Array[Int](selectdim)
    for (i <- 0 until selectdim) topDimArr(i) = topDimArrcalvar(i)._1

    val calDimLenRes = calDimLen(RealData, topDimArr, countRddRealDataNum)
    val splitDim = calDimLenRes._1.sortBy(_._2).reverse.take(1)
    // 그중 길이가 제일 큰거 선택


    val avgDimData = calDimLenRes._2
    var currentSplitDimVar = 0.0
    for (i <- 0 until selectdim) {
      if (topDimArrcalvar(i)._1 == splitDim(0)._1) currentSplitDimVar = topDimArrcalvar(i)._2

    }

    val AvgData = splitDim(0)._3
    if(flag == 0) avgDimData(splitDim(0)._1) = splitDim(0)._5
    else avgDimData(splitDim(0)._1) = splitDim(0)._4
    //(topDimArrcalvar.maxBy(_._2)._2 >= beforeVar)
    if ((RealData.length > thresholdDataNum)) { // (RealData.length > thresholdDataNum)   LimitCount > rpRes.size
      val splitRdd = splitRealRdd(RealData, splitDim(0)._1, AvgData)
      val leftRdd = splitRdd._1
      val rightRdd = splitRdd._2

      val leftCount = leftRdd.length
      val rightCount = rightRdd.length

      if(leftCount == 0 || rightCount == 0){
        rpRes += avgDimData
      }
      else{
        clusteringRdd(leftRdd, currentSplitDimVar, thresholdDataNum, dimension, selectdim,0)
        clusteringRdd(rightRdd, currentSplitDimVar, thresholdDataNum, dimension, selectdim,1)
      }
    }
    else {
      rpRes += avgDimData
    }
  }

  def checkNum(rddRealData: Array[(Array[Double], Int)], splitDim: Int,AvgData: Double): (Int, Int) = {
    var numOfLeft = 0
    var numOfRight = 0
    for (i <- 0 until rddRealData.length) {
      if (rddRealData(i)._1(splitDim) < AvgData ) {
        numOfLeft = numOfLeft + 1
      }
      else {
        numOfRight = numOfRight + 1
      }
    }
    (numOfLeft, numOfRight)
  }

  //rddRealData : Array[Int]

  def splitRealRdd(rddRealData: Array[(Array[Double], Int)], splitDim: Int, AvgData: Double) = {
    val checkNumRes = checkNum(rddRealData, splitDim,AvgData)
    var leftRdd = new Array[(Array[Double], Int)](checkNumRes._1)
    var rightRdd = new Array[(Array[Double], Int)](checkNumRes._2)
    var leftCount = 0
    var rightCount = 0

    for (i <- 0 until rddRealData.length) {
      if (rddRealData(i)._1(splitDim) < AvgData) {
        leftRdd(leftCount) = (rddRealData(i)._1, rddRealData(i)._2)
        leftCount += 1
      }
      else {
        rightRdd(rightCount) = (rddRealData(i)._1, rddRealData(i)._2)
        rightCount += 1
      }
    }
    (leftRdd, rightRdd)
  }

  def calvar(rddRealData: Array[(Array[Double], Int)], countInputData: Int) = {

    val calvarArr = new Array[(Int, Double)](dimension)
    var nAvg: Double = 0
    var dimensionArr = new Array[Double](rddRealData.length)
    var dimensionArr2 = new Array[Double](rddRealData.length)
    for (i <- 0 until dimension) {
      for (j <- 0 until rddRealData.length) {
        var a = rddRealData(j)._1(i)
        dimensionArr(j) = a
        dimensionArr2(j) = a * a
      }
      nAvg = dimensionArr.sum / countInputData

      val vari = dimensionArr2.sum / countInputData
      val resultvar = vari - (nAvg * nAvg)
      calvarArr(i) = (i, resultvar)
    }
    (calvarArr, nAvg)
  }

  def calDimLen(rddRealData: Array[(Array[Double], Int)], topDimArr: Array[Int], countInputData: Int) = {
    val calDimLenResArr = new Array[(Int, Double,Double,Double,Double)](topDimArr.length)
    var sumDimLen = 0.0
    var nAvg: Double = 0
    val avgDimData = new Array[Double](dimension)
    var dataArr = new Array[Double](rddRealData.length)

    var idx = 0
    for (i <- 0 until dimension) {
      for (j <- 0 until rddRealData.length) {
        dataArr(j) = rddRealData(j)._1(i)
      }
      for (k <- 0 until selectdim) {
        if (i == topDimArr(k)) {
          var calDimLenRes = dataArr.max - dataArr.min
          nAvg = dataArr.sum / countInputData
          calDimLenResArr(idx) = (topDimArr(k), calDimLenRes, nAvg, dataArr.max, dataArr.min)
          sumDimLen = sumDimLen + calDimLenRes
          idx += 1
        }
      }
      nAvg = dataArr.sum / countInputData
      avgDimData(i) = nAvg.toInt

    }
    // (차원, 길이) ,  Referencepoint data(128차원),
    (calDimLenResArr, avgDimData)
  }

  def referencepoint(Realdata: Array[(Array[Double], Int)]): Array[Double] = {
    val avgDimData = new Array[Double](dimension)
    var dataArr = new Array[Double](Realdata.length)
    var nAvg: Double = 0
    for (i <- 0 until dimension) {
      for (j <- 0 until Realdata.length) {
        dataArr(j) = Realdata(j)._1(i)
      }
      nAvg = dataArr.sum / Realdata.length
      avgDimData(i) = nAvg.toInt
    }


    avgDimData
  }
}

//기존 IDistance
class IDistance_ReferencePoint3(RealdataRDD : RDD[((Array[Double],Int),Int)]) extends Serializable {


  var dimension = 128  // 16차원 , 128차원
  var selectdim = (dimension * 0.1).toInt
  val featureSize = 2
  var count = 1



  var GroupRDD = RealdataRDD.map(x=>x._1).groupBy(_._2).map(x=>x._2.toArray)
  GroupRDD.persist()

  val thresholdDataNum = (RealdataRDD.count())*0.005
  val beforeVar = 0

  var rpResRDD = GroupRDD.map(
    {x=> var Refe = new ReferencePoint3(x,beforeVar,thresholdDataNum,dimension,selectdim)
      (Refe.rpRes,Refe.PartitionNumber)})

  var rpResArray  =rpResRDD.collect()
  var PartitionNumber = GroupRDD.count().toInt
  var refeaa = new Array[(Array[ReferenceINFO],Int)](PartitionNumber)
  var referenceTestArray = new Array[(Array[ReferenceINFO],Int)](PartitionNumber)
  for(i<-0 until PartitionNumber){
    referenceTestArray(i) = (new Array[ReferenceINFO](rpResArray(i)._1.length),rpResArray(i)._2)
    for(j<-0 until rpResArray(i)._1.length){
      var referenceINFO = new ReferenceINFO(dimension)
      referenceINFO.data = rpResArray(i)._1(j)
      referenceINFO.Partition = rpResArray(i)._2
      referenceTestArray(i)._1(j) = referenceINFO
    }
  }
}
class ReferencePoint3(x: Array[(Array[Double],Int)],beforeVar: Double, thresholdDataNum : Double, dimension:Int, selectdim:Int) extends Serializable {
  val rpRes = new ArrayBuffer[Array[Double]]()
  val PartitionNumber = x(0)._2
  val avgData = new Array[Double](dimension)

  for(i<-0 until dimension){
    var sum = 0.0
    for(j<-0 until x.length){
      sum += x(j)._1(i)
    }
    avgData(i) = sum/x.length
  }
  for(i<-0 until dimension){
    var temp1 = new Array[Double](dimension)
    var temp2 = new Array[Double](dimension)
    for(j<-0 until dimension){
      temp1(j) = avgData(j)
      temp2(j) = avgData(j)
    }
    temp1(i) = 0   // 16차원 : -5 , 128차원 : 0
    temp2(i) = 255  // 16차원 : 50, 128차원 : 255
    rpRes += temp1
    rpRes += temp2
  }

}
