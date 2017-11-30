package org.embulk.output.aerospike

import java.util.concurrent.atomic.AtomicLong

import com.aerospike.client.{AerospikeClient, Bin, Host, Key}
import com.aerospike.client.policy.{ClientPolicy, WritePolicy}
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.spi._
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.Timestamp

import scala.collection.concurrent.TrieMap
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class AerospikePageOutput(taskSource: TaskSource, schema: Schema, taskIndex: Int) extends TransactionalPageOutput {

  import org.embulk.output.aerospike.ops._

  import AerospikeOutputPlugin._

  private[this] val log = Exec.getLogger(classOf[AerospikePageOutput])
  private[this] val tsk = taskSource.loadTask(classOf[AerospikeOutputPlugin.PluginTask])
  private[this] val successCount = new AtomicLong
  private[this] val failCount = new AtomicLong
  private[this] val failures = TrieMap.empty[String, String]

  private[this] val wp: WritePolicy = {
    val wp = new WritePolicy()
    if (tsk.getWritePolicy.isPresent) {
      val wpTask: WritePolicyTask = tsk.getWritePolicy.get
      wp.sendKey = wpTask.getSendKey.get
      wp.expiration = wpTask.getExpiration.get
      wp.maxRetries = wpTask.getMaxRetries.get
      wp.generation = wpTask.getGeneration.get
      wp.sleepBetweenRetries = wpTask.getSleepBetweenRetries.get
    }
    wp
  }

  implicit val policy: ClientPolicy = {
    val cp = new ClientPolicy()
    if (tsk.getClientPolicy.isPresent) {
      val cpTask: ClientPolicyTask = tsk.getClientPolicy.get
      cp.failIfNotConnected = cpTask.getFailIfNotConnected.get
      cp.maxConnsPerNode =cpTask.getMaxConnsPerNode.get()
      cp.maxSocketIdle = cpTask.getMaxSocketIdle.get
      cp.password = cpTask.getPassword.orNull
      cp.user = cpTask.getUser.orNull
      cp.timeout = cpTask.getTimeout.get
      cp.tendInterval = cpTask.getTendInterval.get
    }
    cp.writePolicyDefault = wp
    cp
  }

  val namespace: String = tsk.getNamespace
  val setName: String = tsk.getSetName

  private[this] val hosts: Seq[Host] = tsk.getHosts.map(host => new Host(host.getName, host.getPort))

  private[this] val aerospike = new AerospikeClient(policy, hosts: _*)

  implicit private[this] val reader: PageReader = new PageReader(schema)

  def createRecords(page: Page): Iterator[Seq[Col]] =  {
    reader.setPage(page)
    Iterator.continually(())
      .takeWhile(_ => reader.nextRecord())
      .map(_ => schema.getColumns.toList)
      .map(_.map(Col.of))
  }

  val toRecords: Seq[Col] => java.util.Map[String, Object] = { row =>
    val rec: java.util.Map[String, Object] = new java.util.HashMap()
    row foreach {
      case DoubleColumn(i, n, v) => rec += n -> (v: java.lang.Double)
      case LongColumn(i, n, v) => rec += n -> (v: java.lang.Long)
      case StringColumn(i, n, v) =>
        if (tsk.getSplitters.isPresent) {
          val sps = tsk.getSplitters.get.toMap
          sps.get(n) match {
            case None =>
              rec += n -> (v: java.lang.String)
            case Some(sp) =>
              val sep = sp.getSeparator
              sp.getElementType match {
                case "long" =>
                  val xs = new java.util.ArrayList[Long]
                  v.split(sep).map(s => if (s.isEmpty) "0" else s).foreach(x => xs.add(x.toLong: java.lang.Long))
                  rec += n -> xs
                case "double" =>
                  val xs = new java.util.ArrayList[Double]
                  v.split(sep).map(s => if (s.isEmpty) "0" else s).foreach(x => xs.add(x.toDouble: java.lang.Double))
                  rec += n -> xs
                case "string" =>
                  val xs = new java.util.ArrayList[String]
                  val x = v.split(sep).foreach(xs.add)
                  rec += n -> xs
              }
          }
        } else {
          rec += n -> v
        }
      case BooleanColumn(i, n, v) => rec += n -> (v: java.lang.Boolean)
      case TimestampColumn(i, n, v) => rec += n -> (v.toEpochMilli: java.lang.Long)
      case NullColumn(i, n, t) => // nop
    }
    rec
  }

  def updater(record: java.util.Map[String, Object]): Unit = {
    val keyObj = record.getOrElse(tsk.getKeyName.get, "")
    val key = new Key(namespace, setName, keyObj.toString)
    record.remove(tsk.getKeyName.get)
    Try {
      if (tsk.getSingleBinName.isPresent) {
        val bin = new Bin(tsk.getSingleBinName.get(), record)
        aerospike.put(wp, key, bin)
      } else {
        val bins = record.map { case (k, v) => new Bin(k, v) }
        aerospike.put(wp, key, bins.toSeq: _*)
      }
    } match {
      case Failure(e) =>
        log.error(e.toString, e)
        failures += keyObj.toString -> e.getMessage
        failCount.addAndGet(1L)
      case Success(r) => ()
        successCount.addAndGet(1L)
    }
  }

  val deleter: java.util.Map[String, Object] => Unit = { record =>
    val keyObj = record.getOrElse(tsk.getKeyName.get, "")
    val k = keyObj.toString
    val key = new Key(namespace, setName, k)
    Try(aerospike.delete(wp, key)) match {
      case Failure(e) =>
        log.error(k, e)
        failures += k -> e.getMessage
        failCount.addAndGet(1L)
      case Success(_) =>
        successCount.addAndGet(1L)
    }
  }

  def add(page: Page): Unit = {
    tsk.getCommand match {
      case "put" =>
        createRecords(page).map(toRecords).foreach(updater)
      case "delete" =>
        createRecords(page).map(toRecords).foreach(deleter)
    }
  }

  def finish(): Unit = log.info(s"finish ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def close(): Unit = {
    reader.close()
    aerospike.close()
  }

  def abort(): Unit = log.error(s"abort ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def commit: TaskReport = {
    var r = Exec.newTaskReport
    r.set("rans", successCount.longValue() + failCount.longValue())
    r.set("failures", failures)
    r
  }
}

object ops {

  sealed trait Col

  object Col {
    def of(c: Column)(implicit r: PageReader): Col =
      if (r isNull c) NullColumn(c.getIndex, c.getName, c.getType)
      else c.getType.getName match {
        case "string" =>
          StringColumn(c.getIndex, c.getName, r.getString(c))
        case "double" =>
          DoubleColumn(c.getIndex, c.getName, r.getDouble(c))
        case "long" =>
          LongColumn(c.getIndex, c.getName, r.getLong(c))
        case "boolean" =>
          BooleanColumn(c.getIndex, c.getName, r.getBoolean(c))
        case "timestamp" =>
          TimestampColumn(c.getIndex, c.getName, r.getTimestamp(c))
      }
  }

  case class DoubleColumn(index: Int, name: String, value: Double) extends Col
  case class StringColumn(index: Int, name: String, value: String) extends Col
  case class BooleanColumn(index: Int, name: String, value: Boolean) extends Col
  case class TimestampColumn(index: Int, name: String, value: Timestamp) extends Col
  case class LongColumn(index: Int, name: String, value: Long) extends Col
  case class NullColumn(index: Int, name: String, typ: Type) extends Col

}
