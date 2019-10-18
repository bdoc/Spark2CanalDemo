package com.bdiiot.spark.utils

import com.alibaba.fastjson.JSON
import com.bdiiot.spark.constant.Global._
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ListBuffer

object JsonHelper {

  def jsonValidate(jsonStr: String): Boolean = {
    var flag = false
    try {
      val map = JSON.parseObject(jsonStr)
      if (map.containsKey(DATABASE) &&
        map.containsKey(TABLE) &&
        map.containsKey(TYPE) &&
        map.containsKey(DATA)) {
        flag = true
      }
    } catch {
      case ex: Exception => {
        println(s"$ex")
      }
    }
    flag
  }

  def readJson(jsonStr: String): List[(String, String)] = {
    val valueList = new ListBuffer[(String, String)]()
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

      val key = new StringBuilder()
        .append(database).append("-")
        .append(table).append("-")
        .append(sqlType).toString()

      val dataArray = map.getJSONArray(DATA)
      if (dataArray == null) {
        return List.empty[(String, String)]
      }
      val it = dataArray.iterator()

      while (it.hasNext) {
        val eachRow = it.next().toString
        valueList.append((key, eachRow))
      }
      valueList.toList
    } else {
      valueList.append(("invalid", jsonStr))
      valueList.toList
    }
  }

  def json2Frame(fileNames: Seq[String]) = {
    val sparkSession = SparkHelper.getSparkSession()
    sparkSession.read.json(fileNames: _*)
  }

  def json2Hive(fileNames: Seq[String], hiveTable: String): Unit = {
    try {
      json2Frame(fileNames)
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
