package gdelt.analysis.spark.scala.operations

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.net.URI;

import gdelt.analysis.common.GDELTParser
import gdelt.analysis.common.data.GDELTEvent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


class GDELTInputReceiver(path: String)
  extends Receiver[GDELTEvent](StorageLevel.DISK_ONLY_2) {

  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("GDELT File Receiver") {

      override def run() {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {

  }

  private def receive(): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9001")
    val pt = new Path(path)
    val fs = FileSystem.get(URI.create("hdfs://localhost:9001"),conf);
    var line: String = null
    try {
      val reader = new BufferedReader(new InputStreamReader(fs.open(pt), StandardCharsets.UTF_8))

      val parser = new GDELTParser()

      line = reader.readLine()
      while (!isStopped && line != null) {
        store(parser.readRecord(line))
        line = reader.readLine()
      }
      reader.close()
    } catch {
      case e: Exception =>
        restart("Reading Failed, restarting the receiver", e)
    }

  }
}
