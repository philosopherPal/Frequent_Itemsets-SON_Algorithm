import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.{Set => immutSet}
import scala.collection.mutable.HashMap
import scala.util.Random
import java.io._

object Problem1 {
  def main(args: Array[String]) {

    val start_time = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Pallavi").setMaster("local[2]")
    //.set("spark.default.parallelism", "8")
    //    )
    val case_ = args(0).toInt
    var minSupportSize = args(2).toInt
    val sc = new SparkContext(conf)
    val csv = sc.textFile(args(1),1)
    val header = csv.first()
    val data = csv.filter(row => row != header)
    var baskets:RDD[Set[String]] = null
    if(case_ == 1 ) {
      baskets = data.map(line => line.split(",")).map(inp => (inp(0), inp(1))).groupByKey().map { case (k, v) => v.toSet }
    }
    else if(case_ == 2){
      baskets = data.map(line => line.split(",")).map(inp => (inp(1), inp(0))).groupByKey().map { case (k, v) => v.toSet }
    }
    //val chunkSize = baskets.getNumPartitions
    var mapPhase1 = baskets.mapPartitions( lists => Apriori(lists, minSupportSize ))//.sortByKey()
      .map { case (elemX, elemY) => elemY }.flatMap(item=>(item) map{case (itemSet) =>(itemSet, (itemSet, 1))}
      // for ( itemSet <- a ) yield ( itemSet, (itemSet, 1) ))
    )

    var reducePhase1 = mapPhase1
      .reduceByKey((set1:(Set[String], Int), set2:(Set[String], Int)) => (set1._1,1))
      .map { case (x,y) => y }

    var candidates = reducePhase1.map(x => x._1).collect().toList
    var broadcastOnCluster = sc.broadcast(candidates)
    var mapPhase2 = baskets.flatMap(
      basket => {
        var countInChunk = ListBuffer.empty[Tuple2[Set[String], Int]]
        for (counter <- broadcastOnCluster.value) {
          if (counter.subsetOf(basket.toSet)) {
            countInChunk += Tuple2(counter, 1)
          }
        }
        countInChunk.toList
      })

    var reducePhase2 = mapPhase2.reduceByKey((x,y) => x+y)

    var final_freq = reducePhase2
      .filter(x => x._2 >= minSupportSize)
      .map { case (set1,set2) => (set1.size, List(set1.toList.sorted)) }
      .reduceByKey( (x:List[List[String]],y:List[List[String]]) => x ++: y )
      .map { case (a, tuple) => (a, sort_tuples(tuple)) }
      .sortBy(_._1)
      .map { case (a, tup) => {
        var out = ListBuffer.empty[String]
        for (list <- tup) {
          out += list.mkString("('","','","')")
        }
        out.toList
      } }
    //val result1 = final_freq.map( list => list.mkString(", ") ).collect().foreach(println)
    val result = final_freq.map( list => list.mkString(", ") ).collect()
    val pwrite = new PrintWriter(new File("Pallavi_Taneja_SON_small2.case" + args(0).toString + ".txt" ))
    for (line <- result) {
      pwrite.write(line + "\n")
    }
    pwrite.close()
    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time)/1000 + " secs")
  }


  def generateCombination(candidates: List[Set[String]], size: Int, singleItems: Set[String]): List[Set[String]] = {
    //println("singles" + singleItems)
    var combinations = immutSet.empty[Set[String]]
    for (items <- candidates) {
      for (eachItem <- singleItems) {
        var temp = immutSet() ++ items
        temp += eachItem
        if (temp.size == size) {
          var temp2 = temp.subsets(size - 1).toList
          var flag = true
          if (size > 2) {
            for (itemset <- temp2) {
              if (!candidates.contains(itemset))
                flag = false
            }
          }
          if (flag)
            combinations += temp.toSet
        }

      }
    }
    //val v: List[Set[String]] = combinations.toList
    //println("v" + v)
    combinations.toList
  }

  def filterFrequentItemsets(candidates: List[Set[String]], baskets: List[Set[String]], support: Double): List[Set[String]] = {
    var listOfBaskets = ListBuffer.empty[Set[String]]
    for (eachList <- baskets) {
      listOfBaskets += eachList.toSet
    }
    var ItemSetCount = HashMap.empty[Set[String], Int]
    for (each <- candidates) {
      for (elements <- listOfBaskets) {
        if (each.subsetOf(elements)) {
          if (ItemSetCount.contains(each)) {
            ItemSetCount(each) += 1
          }
          else {
            ItemSetCount += (each -> 1)
          }
        }
      }
    }
    var frequentSets = ListBuffer.empty[Set[String]]
    for (each <- ItemSetCount) {
      if (each._2 >= support) {
        frequentSets += each._1
      }
    }
    frequentSets.toList
  }

  def Apriori(itSet: Iterator[Set[String]], minSupportSizeport: Int): Iterator[(Int, List[Set[String]])] = {

    var baskets = ListBuffer.empty[Set[String]]
    var listBuf = ListBuffer.empty[Set[String]]
    var listOfFrequentSets = List.empty[Set[String]]
    var tuples = HashMap.empty[Int, List[Set[String]]]
    var singletons = Set[String]()
    var singlesCount = HashMap.empty[String, Int]
    //singles
    while (itSet.hasNext) {
      val basket = itSet.next()
      baskets += basket
      for (single <- basket) {
        if (singlesCount.contains(single)) {
          singlesCount(single) += 1
        } else {
          singlesCount += (single -> 1)
        }
      }
    }
    //println("singlesmap" + singleMap)
    // val singletons = singleMap.keySet
    // println(singletons)
    for (singlePair <- singlesCount) {
      if(singlePair._2 >= minSupportSizeport) {
        singletons += singlePair._1
        listBuf += Set(singlePair._1)

      }
    }
    //println("here i am" + singletons)
    var k = 2
    if(!tuples.contains(k-1)) {
      tuples += (k-1 -> listBuf.toList)
    }
    listOfFrequentSets = listBuf.toList
    while (listOfFrequentSets.nonEmpty) {
      listOfFrequentSets = generateCombination(listOfFrequentSets, k, singletons)
      //println(freqSetsList)
      var temp = filterFrequentItemsets(listOfFrequentSets, baskets.toList, minSupportSizeport)
      if (temp.nonEmpty) {
        tuples += (k -> temp)
      }
      listOfFrequentSets = temp
      k = k + 1
    }
    tuples.iterator
  }

  def sort_tuples[A](sorted_list: Seq[Iterable[A]])(implicit ordering: Ordering[A]) = sorted_list.sorted
}


