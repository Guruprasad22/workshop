package com.playground.workshop

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  
  def main(args: Array[String]) {
    
    if (args.length == 3) {
    } else {
    	println("Usage spark-submit --class com.playground.workshop.WordCount <input-path> <output-path> <master>")
    	return
    }
    
    val inPath = args(0)
    val outPath =  args(1)
    val master: String = args(2)
    var sc:SparkContext = null
    
    if (master.equalsIgnoreCase("L")) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sc = new SparkContext("local", "WordCount", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("WordCount")
      sc = new SparkContext(sparkConfig)
    }
    
    val fileRdd = sc.textFile(inPath)
    val wordsRdd = fileRdd.flatMap(record => record.split(" "))
    val wordPairRdd = wordsRdd.map(record => (record,1))
    val countRdd = wordPairRdd.reduceByKey((x,y) => x+y)
    
    countRdd.saveAsTextFile(outPath)
    
    println("______________________done _____________________")
    sc.stop()
  }
}