package com.bdiiot.spark.utils

import com.alibaba.fastjson.JSON

import scala.collection.mutable.ListBuffer

object JsonHelper {

  def enableToJson(jsonStr: String): Boolean = {
    var flag = false
    try {
      JSON.parseObject(jsonStr)
      flag = true
    } catch {
      case ex: Exception => {
        println(s"$ex")
      }
    }
    flag
  }

  def readJson(jsonStr: String): List[(String, String)] = {
    val valueList = new ListBuffer[(String, String)]()
    if (enableToJson(jsonStr)) {
      val map = JSON.parseObject(jsonStr)

      val database = {
        val temp = map.getString("database")
        if (temp == null) "none" else temp
      }
      val table = {
        val temp = map.getString("table")
        if (temp == null) "none" else temp
      }
      val sqlType = {
        val temp = map.getString("type")
        if (temp == null) "none" else temp
      }

      val key = new StringBuilder()
        .append(database).append("-")
        .append(table).append("-")
        .append(sqlType).toString()

      val dataArray = map.getJSONArray("data")
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
      valueList.append(("key", "eachRow"))
      valueList.toList
    }
  }

}
