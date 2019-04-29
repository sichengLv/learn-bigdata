import com.lv.learn.scala.Base

/**
  * 单词计数
  */
object WordCount extends Base {

  def testCombiner() = {
    val rdd = sc.parallelize(Array((1, 2), (2, 1), (1, 1), (2, 5), (3, 4), (3, 1)), 3)
    val result = rdd.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }

    result.collectAsMap().map(println(_))
  }

  def wordCount() = {
    val rdd = sc.textFile("g:/work/word.txt")
    val resRdd = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    resRdd.collect().foreach(println(_))
  }

  def main(args: Array[String]): Unit = {
    testCombiner()
  }

}