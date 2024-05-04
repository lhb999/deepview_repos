package Master
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import util.control.Breaks._
import scala.util.control._
import org.apache.spark.broadcast.Broadcast


object Quantization {
  //def printArray[K](array: Array[K]) = println(array.mkString("Array(", ", ", ")"))//,broadHdi:Broadcast[Master.HDIClass]
  def processQuantization(
                           param_RealDataRDD: RDD[Array[Int]],
                           broadHdi:Broadcast[Master.HDIClass]
                         ) = {
    println("\nINFO::Real Data Load......")
    val realDataRDD = param_RealDataRDD

    println("INFO::Data quantization......\n")
    realDataRDD.map(x => calQuantData(x,broadHdi))
    //quantizatedRDD.map(x=>x.toList).coalesce(1).saveAsTextFile("file:////home/etri/code/log/newquantValueCheck/")
  }

  def calQuantData(
                    realData : Array[Int],
                    broadHdi : Broadcast[Master.HDIClass]
                  ): Array[((Int, Int), Int)] = {
    val HDI = broadHdi.value //map안에서는 이렇게 사용해주어야 한다.
    val quantArr = new Array[((Int, Int), Int)](HDI.dimension)
    for (dim <- 0 until HDI.dimension) {
      var quantData = (realData(dim) / HDI.voc).toInt
      if (quantData >= HDI.quant_value) quantData = HDI.quant_value - 1
      quantArr(dim) = ((dim, quantData), 1)
    } //어차피 양자화는 통계, 즉 분산 계산을 위해 존재하는거니까 처음에 양자화 자체만 존재하는게 아닌 (차원,양자화값),개수를위한 카운트 value 형식으로 저장하게한다.
    quantArr
  }

  //분산 계산
  def calVariance(
                   quantDataRDDforVar : RDD[((Int, Int), Int)],
                   data_number        : Int,node:Node
                 ):(Int,Int) = {
    println("INFO::Variance caculation.......")

    quantDataRDDforVar.cache()

    //분산 값을 내림차순 정렬
    val dimensionVariance = quantDataRDDforVar
      .groupBy(_._1._1) //dimension별로 groupby해서
      .map(x=>x._2) // groupBy하면 (그룹의 기준,원래데이터) 이렇게 되니까 다시 원레데이터만취해가지고
      .map(x=>variance(x.toArray)) //분산을 계산해줌 (차원,분산값)

    val sortedRDD = dimensionVariance.sortBy(_._2, ascending = false)

    val maxVariance = sortedRDD.map(x=>x._1).collect

    val (split_dim,split_quantval) = Quantization.splitCheck(
      quantDataRDDforVar,
      maxVariance,
      data_number,
      node
    )

    quantDataRDDforVar.unpersist()
    (split_dim,split_quantval)
  }

  //분산계산하는함수
  def variance(datas:Array[((Int,Int),Int)])={ //dimension마다 이렇게 구함
    val dimension = datas.map(x=>x._1._1).take(1)(0)

    val countOfFeature  = datas.groupBy(_._1).size //feature몇개
    val mean = datas
      .groupBy(_._1._1)
      .mapValues(x=>(x.map(_._2).sum)/countOfFeature)
      .take(1)(dimension) //이 차원의 featrue 전체모든 데이터의 합 다가 개수 나눠서 평균

    val subOfData  = datas.groupBy(_._1)
      .mapValues(data => {
        val sumOfData = data.map(_._2).sum //feature별 데이터 개수
        val sub       = Math.pow(mean-sumOfData,2.0)
        sub
      }) //((dimension[고정],차원)->편차)

    val allSubSum = subOfData
      .groupBy(_._1._1)
      .mapValues(x=>x.map(_._2).sum)
      .take(1)(dimension)

    val result = allSubSum/countOfFeature //편차의제곱의합/변량의 개수

    (dimension,result)
  }


  //split check : 양쪽 노드의 데이터 개수가 ratio_number 미만인지 체크
  def splitCheck(
                  quantDataRDDforVar : RDD[((Int, Int), Int)],
                  varArr             : Array[Int],
                  data_number        : Int,
                  node               : Node
                ):(Int,Int)={
    println("\nINFO::Split check........")
    var split_dim   = 0
    var split_quant = 0
    val ratio_number = data_number * main.HDI.ratio

    val vararr = varArr.map(_.toInt)
    breakable {
      for (dim <- vararr) {
        split_dim = dim
        for (quant <- 0 until main.HDI.quant_value) {
          split_quant = quant

          val leftData = quantDataRDDforVar
            .filter(x => x._1._1 == dim && x._1._2 <= quant)
          val leftSum = leftData
            .map(x => x._2)
            .sum()
          val diff = data_number - (leftSum*2)

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
