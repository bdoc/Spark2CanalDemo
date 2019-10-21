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

      // key=(database)-(table)-(sort)-(type)
      val tableInfos = dirPath.getName.substring(4).split("-")
      if (tableInfos(0).equals("none") || tableInfos(1).equals("none")) {
        tableInfos(3) = "none"
      }
      val hiveTable = new StringBuilder(tableInfos(0))
        .append(DB_MARK)
        .append(tableInfos(1))
        .toString()

      val ifTableExists = SparkHelper.getSparkSession()
        .sql(s"show tables in $DB_NAME")
        .toDF("database", "tableName", "isTemporary")
        .where(col("tableName") === hiveTable)
        .count().equals(1L)

      val fullTableName = DB_NAME.concat(".").concat(hiveTable)
      println(fullTableName)

      if (ifTableExists) {
        if (tableInfos(3) == "INSERT") {
          JsonHelper.json2Hive(fileNames, fullTableName)
        } else if (tableInfos(3) == "UPDATE") {
          updateFromFiles(fileNames, fullTableName)
        } else if (tableInfos(3) == "DELETE") {
          deleteFromFiles(fileNames, fullTableName)
        } else {
          getFileSystem().mkdirs(new Path(PATH_INVALID))
          getFileSystem().rename(dirPath, new Path(PATH_INVALID, dirPath.getName))
        }
      } else {
        println(fullTableName.concat(" is not exists!"))
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

      val sumData = oldData
        .join(deleteData, Seq(COL_COMMON_KEY), LEFT_ANTI)
        .select(oldData("*"))
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
