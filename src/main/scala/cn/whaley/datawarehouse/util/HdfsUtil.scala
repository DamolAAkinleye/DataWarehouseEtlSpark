package cn.whaley.datawarehouse.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, FileStatus, FileSystem, Path}

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

  def pathIsExist(file:String): Boolean = {
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val path = new Path(file)
    fs.exists(path)
  }

  def IsDirExist(path:String):Boolean={
    var flag = false
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    flag=fs.exists(new Path(path))
    flag
  }

  def copyFilesInDir(srcDir:String,distDir:String) :Boolean = {
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val isSuccess=FileUtil.copy(fs,new Path(srcDir),fs,new Path(distDir),false,false,conf)
    isSuccess
  }

  def rename(src:String,dist:String) :Boolean ={
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val srcPath = new Path(src)
    val distPath = new Path(dist)
    fs.rename(srcPath,distPath)
  }

}
