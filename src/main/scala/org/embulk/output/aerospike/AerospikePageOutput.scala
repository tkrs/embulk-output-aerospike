package org.embulk.output.aerospike

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import com.aerospike.client.{AerospikeException, Bin, Host, Key}
import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy}
import com.aerospike.client.listener.{DeleteListener, WriteListener}
import com.aerospike.client.policy.WritePolicy
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.spi._
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.Timestamp

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class AerospikePageOutput(taskSource: TaskSource, schema: Schema, taskIndex: Int) extends TransactionalPageOutput {

  import org.embulk.output.aerospike.ops._

  import AerospikeOutputPlugin._

  private[this] val log = Exec.getLogger(classOf[AerospikePageOutput])
  private[this] val tsk = taskSource.loadTask(classOf[AerospikeOutputPlugin.PluginTask])
  private[this] val successCount = new AtomicLong
  private[this] val failCount = new AtomicLong
  private[this] val failures = ListBuffer.empty[String]

  private[this] val wp: WritePolicy = {
    val p = new WritePolicy()
    tsk.getWritePolicy.transform({ (wpTask: WritePolicyTask) =>
      wpTask.getSendKey.transform((x: Boolean) => p.sendKey = x)
      wpTask.getExpiration.transform((x: Integer) => p.expiration = x)
      wpTask.getMaxRetries.transform((x: Integer) => p.maxRetries = x)
      wpTask.getGeneration.transform((x: Integer) => p.generation = x)
      wpTask.getSleepBetweenRetries.transform((x: Integer) => p.sleepBetweenRetries = x)
      p
    }).or(p)
  }

  private[this] val policy: AsyncClientPolicy = {
    val p = new AsyncClientPolicy()
    p.writePolicyDefault = wp
    tsk.getClientPolicy.transform({ (cpTask: ClientPolicyTask) =>
      cpTask.getFailIfNotConnected.transform((x: Boolean) => p.failIfNotConnected = x)
      cpTask.getMaxSocketIdle.transform((x: Integer) => p.maxSocketIdle = x)
      cpTask.getPassword.transform((x: String) => p.password = x)
      cpTask.getUser.transform((x: String) => p.user = x)
      cpTask.getTimeout.transform((x: Integer) => p.timeout = x)
      cpTask.getMaxThreads.transform((x: Integer) => p.maxConnsPerNode = x)
      cpTask.getMaxConnsPerNode.transform((x: Integer) => p.maxConnsPerNode = x)
      cpTask.getTendInterval.transform((x: Integer) => p.tendInterval = x)
      p
    }).or(p)
  }

  private[this] val hosts: Seq[Host] = tsk.getHosts.asScala.map(host => new Host(host.getName, host.getPort))
  private[this] val client = new AsyncClient(policy, hosts: _*)

  implicit private[this] val reader: PageReader = new PageReader(schema)

  val createRecords: Page => Seq[Vector[Col]] = { page =>
    reader.setPage(page)
    val records: ListBuffer[Vector[Col]] = ListBuffer.empty
    while (reader.nextRecord())
      records += (for (col <- schema.getColumns.asScala.toVector) yield Col of col)
    records
  }

  val toRecords: Vector[Col] => Map[String, Any] = { row =>
    row.foldLeft(List.empty[(String, Any)])({
      case (acc, DoubleColumn(_, n, v)) => n -> v :: acc
      case (acc, LongColumn(_, n, v)) => n -> v :: acc
      case (acc, StringColumn(_, n, v)) =>
        tsk.getSplitters.transform({ (t: java.util.Map[String, SplitterTask]) =>
          t.asScala.toMap.get(n).fold(n -> v :: acc) { sp =>
            val sep = sp.getSeparator
            sp.getElementType match {
              case "long" =>
                val x = v.split(sep).toSeq.map(s => if (s.isEmpty) "0" else s).map(_.toLong)
                n -> x :: acc
              case "double" =>
                val x = v.split(sep).toSeq.map(s => if (s.isEmpty) "0" else s).map(_.toDouble)
                n -> x :: acc
              case "string" =>
                val x = v.split(sep).toSeq
                n -> x :: acc
            }
          }
        }).or(n -> v :: acc)
      case (acc, BooleanColumn(_, n, v)) => n -> v :: acc
      case (acc, TimestampColumn(_, n, v)) => n -> v :: acc
      case (acc, NullColumn(_, _, _)) => acc
    }).toMap
  }

  def updater(wl: WriteListener)(record: Map[String, Any]): Unit = {
    val keyObj = record.getOrElse(tsk.getKeyName.get, "")
    val key = new Key(tsk.getNamespace, tsk.getSetName, keyObj.toString)
    val deRec = record - tsk.getKeyName.get
    val bins = tsk.getSingleBinName.transform(
      (name: String) => Seq(new Bin(name, deRec.asJava))
    ).or(
      deRec.map {case (name, value) => new Bin(name, value) }.toSeq
    )

    client.put(wp, wl, key, bins: _*)
  }

  def deleter(wl: DeleteListener)(record: Map[String, Any]): Unit = {
    val keyObj = record.getOrElse(tsk.getKeyName.get, "")
    val key = new Key(tsk.getNamespace, tsk.getSetName, keyObj.toString)
    client.delete(wp, wl, key)
  }


  def add(page: Page): Unit = {
    val records = createRecords(page)
    val latch = new CountDownLatch(records.size)
    tsk.getCommand match {
      case "put" =>
        val listener =  new WriteListener {
          override def onFailure(e: AerospikeException): Unit = {
            log.error(e.toString, e)
            failures += e.getMessage
            failCount.addAndGet(1L)
            latch.countDown()
          }

          override def onSuccess(key: Key): Unit = {
            successCount.addAndGet(1L)
            latch.countDown()
          }
        }
        records.map(toRecords).foreach(updater(listener))
      case "delete" =>
        val listener =  new DeleteListener {
          override def onFailure(e: AerospikeException): Unit = {
            log.error(e.toString, e)
            failures += e.getMessage
            failCount.addAndGet(1L)
            latch.countDown()
          }
          override def onSuccess(key: Key, existed: Boolean): Unit = {
            successCount.addAndGet(1L)
            latch.countDown()
          }
        }
        records.map(toRecords).foreach(deleter(listener))
    }
  }

  def finish(): Unit =
    log.info(s"finish ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def close(): Unit = {
    reader.close()
    client.close()
  }

  def abort(): Unit =
    log.error(s"abort ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def commit: TaskReport = {
    val r = Exec.newTaskReport
    r.set("rans", successCount.longValue() + failCount.longValue())
    r.set("failures", failures.asJava)
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
