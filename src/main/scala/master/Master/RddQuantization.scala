package Master
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import util.control.Breaks._
import scala.util.control._
import org.apache.spark.broadcast.Broadcast


object RddQuantization {
  //def printArray[K](array: Array[K]) = println(array.mkString("Array(", ", ", ")"))//,broadHdi:Broadcast[Master.HDIClass]
  def processQuantization(param_RealDataRDD: RDD[Array[Int]],broadHdi:Broadcast[Master.HDIClass]) = {

    println("데이터 개수"+param_RealDataRDD.count())
    println("\nINFO::Real Data Load......")
    val realDataRDD = param_RealDataRDD

    println("INFO::Data quantization......\n")
    realDataRDD.map(x => calQuantData(x,broadHdi))
    //quantizatedRDD.map(x=>x.toList).coalesce(1).saveAsTextFile("file:////home/etri/code/log/newquantValueCheck/")
  }

  def calQuantData(realData: Array[Int],broadHdi:Broadcast[Master.HDIClass]): Array[((Int, Int), Int)] = {
    val HDI=broadHdi.value //map안에서는 이렇게 사용해주어야 한다.
    val quantArr = new Array[((Int, Int), Int)](HDI.dimension)
    for (dim <- 0 until HDI.dimension) {
      var quantData = (realData(dim) / HDI.voc).toInt
      if (quantData >= HDI.quant_value) quantData = HDI.quant_value - 1
      quantArr(dim) = ((dim, quantData), 1)
    } //어차피 양자화는 통계, 즉 분산 계산을 위해 존재하는거니까 처음에 양자화 자체만 존재하는게 아닌 (차원,양자화값),개수를위한 카운트 value 형식으로 저장하게한다.
    quantArr
  }

  //분산 계산
  def calVariance(quantDataRDDforVar: RDD[((Int, Int), Int)], data_number: Int,node:RddNode):(Int,Int) = {
    println("INFO::Variance caculation.......")
    val dataMean =data_number / main.HDI.quant_value
    quantDataRDDforVar.cache()
    //분산 값을 내림차순 정렬
    val sortedArr= quantDataRDDforVar.map(x=>((x._2-dataMean)*(x._2-dataMean),x._1._1)).sortByKey()

    val (split_dim,split_quantval) = RddQuantization.splitCheck(quantDataRDDforVar, sortedArr.map(x=>x._2).collect(), data_number,node)
    quantDataRDDforVar.unpersist()
    (split_dim,split_quantval)
  }


  //split check : 양쪽 노드의 데이터 개수가 ratio_number 미만인지 체크
  def splitCheck(quantDataRDDforVar: RDD[((Int, Int), Int)], varArr: Array[Int], data_number: Int,node:RddNode):(Int,Int)={
    println("\nINFO::Split check........")
    var split_dim = 0
    var split_quant =0
    val ratio_number = data_number * main.HDI.ratio

    breakable {
      for (dim <- varArr) {
        split_dim=dim
        for (quant <- 0 until main.HDI.quant_value) {
          split_quant=quant

          val leftData = quantDataRDDforVar.filter(x => x._1._1 == dim && x._1._2 <=quant)
          val leftSum = leftData.map(x => x._2).sum()
          val diff = data_number -(leftSum*2)

          if ((diff.abs) <= ratio_number) {
            println("-------------split check INFO--------------"
              + "\n\tsplit dimension : " + dim
              + "\n\tsplit quant value : " + quant
              + "\n\tsplit original value(save) : " + (quant+1) * main.HDI.voc
              + "\n\tdiff value : " + diff.abs
              + "\n\tratio number : " + ratio_number
              +"\n-------------------------------------------")
            break()
          }
        }
      }
    }
    (split_dim,split_quant)
  }
}
