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
        val sparkSession = SparkHelper.getSparkSession()
        val dbTable = sparkSession.read.table(DB_TABLE)
          .where(col(BD_KEY) === tableInfos(0))
          .where(col(TABLE_KEY) === tableInfos(1))

        val primaryKey = dbTable.select(PRIMARY_KEY).head().getString(0)
        val modifiedDate = dbTable.select(UPDATE_TIME_KEY).head().getString(0)
        println("Key: ".concat(primaryKey.concat(modifiedDate)))

        if (tableInfos(3) == "INSERT") {
          JsonHelper.json2Hive(fileNames, fullTableName)
        } else if (tableInfos(3) == "UPDATE") {
          updateFromFiles(fileNames, fullTableName, primaryKey, modifiedDate)
        } else if (tableInfos(3) == "DELETE") {
          deleteFromFiles(fileNames, fullTableName, primaryKey)
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

  def deleteFromFiles(fileNames: Array[String], hiveTable: String, primaryKey: String): Unit = {
    try {
      val sparkSession = SparkHelper.getSparkSession()
      val oldData = sparkSession.read.table(hiveTable)
      println("oldData")
      oldData.show()

      val deleteData = JsonHelper.json2Frame(fileNames, hiveTable)
      println("deleteData")
      deleteData.show()

      val sumData = oldData
        .join(deleteData, Seq(primaryKey), LEFT_ANTI)
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

  def updateFromFiles(fileNames: Array[String], hiveTable: String, primaryKey: String, modifiedDate: String): Unit = {
    try {
      val sparkSession = SparkHelper.getSparkSession()
      val oldData = sparkSession.read.table(hiveTable)
      println("oldData")
      oldData.show()

      val updatedData = JsonHelper.json2Frame(fileNames, hiveTable)
        .withColumn(FLAG_KEY,
          row_number().over(Window.partitionBy(primaryKey).orderBy(col(modifiedDate) desc)))
        .where(col(FLAG_KEY) === 1)
        .drop(FLAG_KEY)
      println("updatedData")
      updatedData.show()

      val sumData = oldData
        .join(updatedData, Seq(primaryKey), LEFT_OUTER)
        .where(updatedData(modifiedDate).isNull)
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
