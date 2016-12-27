package cn.whaley.datawarehouse.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
  * Created by Tony on 16/12/22.
  */
object HdfsUtil {

  def deleteHDFSFileOrPath(file:String){
    val conf = new Configuration()
    val fs= FileSystem.get(conf)

    val path = new Path(file)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }

  def getHDFSFileStream(file:String) = {
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val path = new Path(file)
    fs.open(path)
  }

  def getFileFromHDFS(path:String):Array[FileStatus]={
    val dst = path
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val input_dir = new Path(dst)
    val hdfs_files = fs.listStatus(input_dir)
    hdfs_files
  }

  def fileIsExist(path:String,fileName:String)={
    var flag = false
    val files = getFileFromHDFS(path)
    files.foreach(file=>{
      if(file.getPath.getName==fileName){
        flag = true
      }
    })
    flag
  }
}
