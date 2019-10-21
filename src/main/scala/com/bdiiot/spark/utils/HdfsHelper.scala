package com.bdiiot.spark.utils

import java.net.URI

import com.bdiiot.spark.constant.Global._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class HdfsHelper(pathStr: String) {

  import HdfsHelper._

  val path: Path = {
    println(pathStr)
    new Path(pathStr)
  }

  def dir2Hive: Unit = {
    if (!isHdfsDir(path)) {
      return
    }

    val dirs: Array[FileStatus] = scanDir(path, "key=", TRUE)
    for (dir <- dirs) {
      val dirPath = dir.getPath
      val fileNames = scanDir(dirPath, ".txt", FALSE)
        .map(fileStatus => fileStatus.getPath.toString)
      fileNames.foreach(println)

      // key=(database)-(table)-(type)
      val tableInfos = dirPath.getName.substring(4).split("-")
      if (tableInfos(0).equals("none") || tableInfos(1).equals("none")) {
        tableInfos(2) = "none"
      }
      val hiveTable = new StringBuilder("canal.")
        .append(tableInfos(0))
        .append("_bd_")
        .append(tableInfos(1))
        .toString()
      println(hiveTable)

      if (tableInfos(2) == "INSERT") {
        JsonHelper.json2Hive(fileNames, hiveTable)
      } else if (tableInfos(2) == "UPDATE") {
        updateFromFiles(fileNames, hiveTable)
      } else if (tableInfos(2) == "DELETE") {
        deleteFromFiles(fileNames, hiveTable)
      } else {
        getFileSystem().mkdirs(new Path(PATH_INVALID))
        getFileSystem().rename(dirPath, new Path(PATH_INVALID, dirPath.getName))
      }

      delIfExists(dir.getPath)
    }

  }

}

object HdfsHelper {

  def apply(path: String): HdfsHelper = new HdfsHelper(path)

  def deleteFromFiles(fileNames: Array[String], hiveTable: String): Unit = {

    try {
      val deleteData = JsonHelper.json2Frame(fileNames)
      println("deleteData")
      deleteData.show()

      val sparkSession = SparkHelper.getSparkSession()
      val oldData = sparkSession.read.table(hiveTable)
      println("oldData")
      oldData.show()

      //      val sumData = oldData
      //        .join(deleteData, Seq(COL_COMMON_KEY), LEFT_OUTER)
      //        //.where(deleteData(COL_COMMON_KEY).isNull)
      //        .select(oldData("*"))
      //      println("sumData")
      //      sumData.show()
      oldData.createTempView("oldData")
      deleteData.createTempView("deleteData")

      val sumData = sparkSession.sql("select o.* from oldData o left join deleteData d on o.id=d.id where d.id is null")
      println("sumData")
      sumData.show()

      sumData
        .checkpoint(TRUE)
        .repartition(REPARTITION)
        .write.format(HIVE_SOURCE)
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTable)
    } catch {
      case ex: Exception => {
        println(s"$ex")
      }
    }
  }

  def updateFromFiles(fileNames: Array[String], hiveTable: String): Unit = {


    try {
      val updatedData = JsonHelper.json2Frame(fileNames)
        .withColumn(COL_FLAG_KEY,
          row_number().over(Window.partitionBy(COL_COMMON_KEY).orderBy(col(COL_COMMON_UPDATE_TIME) desc)))
        .where(col(COL_FLAG_KEY).===(1))
        .drop(COL_FLAG_KEY)
      println("updatedData")
      updatedData.show()

      val sparkSession = SparkHelper.getSparkSession()
      val oldData = sparkSession.read.table(hiveTable)
      println("oldData")
      oldData.show()

      val sumData = oldData
        .join(updatedData, Seq(COL_COMMON_KEY), LEFT_OUTER)
        .where(updatedData(COL_COMMON_UPDATE_TIME).isNull)
        .select(oldData("*"))
        .union(updatedData)
      println("sumData")
      sumData.show()

      sumData
        .checkpoint(TRUE)
        .repartition(REPARTITION)
        .write.format(HIVE_SOURCE)
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTable)
    } catch {
      case ex: Exception => {
        println(s"$ex")
      }
    }
  }

  def scanDir(dir: Path, filter: String, isHead: Boolean) = {
    isHdfsDir(dir)
    getFileSystem().listStatus(dir, new PathFilter {
      override def accept(path: Path): Boolean = {
        if (isHead) {
          path.getName.startsWith(filter)
        } else {
          path.getName.endsWith(filter)
        }
      }
    })
  }

  def isHdfsDir(path: Path): Boolean = {
    getFileSystem().exists(path) && getFileSystem().isDirectory(path)
  }

  def delIfExists(path: Path) = {
    getFileSystem().delete(path, true)
  }

  private var singleDfs: FileSystem = null

  def getFileSystem(): FileSystem = {
    if (singleDfs == null) {
      synchronized {
        if (singleDfs == null) {
          singleDfs = FileSystem.get(
            new URI(HDFS),
            SparkHelper.getSparkSession().sparkContext.hadoopConfiguration)
        }
        println(singleDfs + " HDFS FileSystem connect success!")
      }
    }
    singleDfs
  }

  def close(): Unit = {
    if (singleDfs != null) {
      try {
        singleDfs.close()
      } catch {
        case ex: Exception => {
          println(s"close singled dfs failed, msg=$ex")
        }
      }
    }
  }
}
