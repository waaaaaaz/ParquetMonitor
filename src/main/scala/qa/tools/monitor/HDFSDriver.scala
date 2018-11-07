package qa.tools.monitor

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}


class HDFSDriver() {

  private val baseHDFSAddress = Configuration.baseHDFSAddress
  private val conf = new Configuration()
  private val hdFS = FileSystem.get(new URI(baseHDFSAddress), conf)

  def filesStatus(filePathStr: String): Array[FileStatus] = {
    val hdFSPath = new Path(filePathStr)
    fileListRecursion(hdFSPath)
  }

  private def fileListRecursion(filePath: Path): Array[FileStatus] = {
    val (directories, files) = hdFS.listStatus(filePath).partition(_.isDirectory)
    files ++ directories.flatMap(f => fileListRecursion(f.getPath))
  }

  def parquetFilesStatus(filePathStr: String): Array[FileStatus] = {
    filesStatus(filePathStr).filter(_.getPath.getName.endsWith("parquet"))
  }

  def targetParquetFilesStatus(filePathStr: String, lastExeTime: Long): Array[FileStatus] = {
    parquetFilesStatus(filePathStr).filter(_.getModificationTime > lastExeTime)
  }

  def parquetFilesSortedList(filePathStr: String): Array[String] = {
    filesStatus(filePathStr)
      .filter(_.getPath.getName.endsWith("parquet"))
      .sortWith(_.getModificationTime < _.getModificationTime)
      .map(x => x.getPath.toString)
  }

  def targetParquetFilesSortedList(filePathStr: String, lastExeTime: Long): Array[String] = {
    parquetFilesStatus(filePathStr)
      .filter(_.getModificationTime > lastExeTime)
      .sortWith(_.getModificationTime < _.getModificationTime)
      .map(x => x.getPath.toString)
  }

}
