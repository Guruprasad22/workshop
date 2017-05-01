package com.playground.workshop

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.Row;


object MyDataFrame {
  
  def main(args: Array[String]) {
    
    if (args.length == 4) {
      println("_____________________start______________________________")
    } else {
    	println("Usage spark-submit --class com.playground.workshop.WordCount <input-path1> <input-path2> <output-path> <master>")
    	return
    }
    
    val empPath = args(0)
    val depPath = args(1)
    val outPath =  args(2)
    val master = args(3)
    
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
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val empRdd = sc.textFile(empPath)
    val depRdd = sc.textFile(depPath)
    
    /*case class Employee(id: Int, name: String, deptId: Int)
    case class Department(id: Int, name: String)
    
    val empDF = empRdd.map(_.split(",")).map(rec => Employee(rec(0),rec(1),rec(2)).toDF()
    val deptDF = depRdd.map(_.split(",")).map(x => Department(x(0),x(1)).toDF*/
    
    val empSchemaString = "id name deptId"
    val deptSchemaString = "id name"
    val empSchema =   StructType(empSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val deptSchema = StructType(deptSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //Convert records of the RDD (people) to Rows.
    val rowEmpRDD = empRdd.map(x => x.split(",")).map(rec => Row(rec(0), rec(1), rec(2)))
    val rowDeptRDD = depRdd.map(x => x.split(",")).map(rec => Row(rec(0), rec(1)))

    // Apply the schema to the RDD.
    val empDF = sqlContext.createDataFrame(rowEmpRDD, empSchema)
    val deptDF = sqlContext.createDataFrame(rowDeptRDD, deptSchema)
    
    empDF.show()
    deptDF.show()
    
    val empDeptDF = empDF.join(deptDF,empDF("deptId") === deptDF("id"))
    
    empDeptDF.show()
    
    println("___________________________________end___________________________________")
    
  }
  
}