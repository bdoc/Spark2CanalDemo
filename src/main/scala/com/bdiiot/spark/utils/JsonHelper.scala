package com.bdiiot.spark.utils

import com.alibaba.fastjson.JSON
import com.bdiiot.spark.constant.Global._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer

object JsonHelper {

  def jsonValidate(jsonStr: String): Boolean = {
    var flag = false
    try {
      val map = JSON.parseObject(jsonStr)
      if (
        map.containsKey(DATABASE) &&
          map.containsKey(TABLE) &&
          map.containsKey(TYPE) &&
          map.containsKey(DATA)
      ) {
        flag = true
      }
    } catch {
      case ex: Exception => {
        println(s"$ex")
      }
    }
    flag
  }

  def readJson(jsonStr: String): List[(String, String, String)] = {
    val valueList = new ListBuffer[(String, String, String)]()
    if (jsonValidate(jsonStr)) {
      val map = JSON.parseObject(jsonStr)

      val database = {
        val temp = map.getString(DATABASE)
        if (temp == null) "none" else temp
      }
      val table = {
        val temp = map.getString(TABLE)
        if (temp == null) "none" else temp
      }
      val sqlType = {
        val temp = map.getString(TYPE)
        if (temp == null) "none" else temp
      }

      val tableName = "ods." + database + "_bd_" + table

      val dataArray = map.getJSONArray(DATA)
      if (dataArray == null) {
        return List.empty[(String, String, String)]
      }
      val it = dataArray.iterator()

      while (it.hasNext) {
        val eachRow = it.next().toString
        valueList.append((tableName, sqlType, eachRow))
      }
      valueList.toList
    } else {
      valueList.append(("invalid", "none", jsonStr))
      valueList.toList
    }
  }

  def json2Frame(fileNames: Seq[String], hiveTable: String) = {
    val sparkSession = SparkHelper.getSparkSession()
    var readJson = sparkSession.read.json(fileNames: _*)
    val tableColumns = sparkSession.read.table(hiveTable).columns
    val jsonColumns = readJson.columns

    val tableColumn_ = tableColumns
    val jsonColumns_ = jsonColumns
    println("tableColumns_: ".concat(tableColumn_.mkString(",")))
    println("jsonColumns_: ".concat(jsonColumns_.mkString(",")))

    for (columns <- tableColumns) {
      if (!jsonColumns.contains(columns)) {
        println("columns: ".concat(columns))
        readJson = readJson.withColumn(columns, lit(null: String).cast(StringType))
        println("----------------")
      }
    }
    println("jsonColumns: ".concat(readJson.columns.mkString(",")))
    println("==================")

    readJson.select(tableColumns.head, tableColumns.tail: _*)
  }

  def json2Hive(fileNames: Seq[String], hiveTable: String): Unit = {
    try {
      json2Frame(fileNames, hiveTable)
        .repartition(REPARTITION)
        .write.format(HIVE_SOURCE)
        .mode(SaveMode.Append)
        .saveAsTable(hiveTable)
    } catch {
      case ex: Exception => {
        println(s"$ex")
      }
    }
  }

}
