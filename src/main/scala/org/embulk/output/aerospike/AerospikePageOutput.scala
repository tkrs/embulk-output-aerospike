package org.embulk.output.aerospike

import java.util.concurrent.{ CountDownLatch, ConcurrentLinkedQueue }
import java.util.concurrent.atomic.AtomicLong

import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.{ DeleteError, PutError, Aerospike }
import cats.data.Xor, Xor._
import io.circe._, io.circe.syntax._
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.spi._
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.Timestamp

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{ Map => MMap, ListBuffer }
import scala.collection.JavaConversions._
import scalaz.{ \/-, -\/ }
import scalaz.concurrent.Task
import scalaz.stream._

class AerospikePageOutput(taskSource: TaskSource, schema: Schema, taskIndex: Int) extends TransactionalPageOutput {

  import org.embulk.output.aerospike.ops._

  import AerospikeOutputPlugin._

  private[this] val log = Exec.getLogger(classOf[AerospikePageOutput])
  private[this] val tsk = taskSource.loadTask(classOf[AerospikeOutputPlugin.PluginTask])
  private[this] val successCount = new AtomicLong
  private[this] val failCount = new AtomicLong
  private[this] val failures = TrieMap.empty[String, String]

  private[this] val wp: WritePolicy = {
    if (tsk.getWritePolicy.isPresent) {
      val wpTask: WritePolicyTask = tsk.getWritePolicy.get
      WritePolicy(
        sendKey = wpTask.getSendKey.get,
        expiration = wpTask.getExpiration.get,
        maxRetries = wpTask.getMaxRetries.get,
        generation = wpTask.getGeneration.get,
        sleepBetweenRetries = wpTask.getSleepBetweenRetries.get
      )
    } else {
      WritePolicy()
    }
  }

  implicit val policy: ClientPolicy = {
    if (tsk.getClientPolicy.isPresent) {
      val cpTask: ClientPolicyTask = tsk.getClientPolicy.get
      ClientPolicy(
        failIfNotConnected = cpTask.getFailIfNotConnected.get,
        maxThreads = cpTask.getMaxThreads.get,
        maxSocketIdle = cpTask.getMaxSocketIdle.get,
        password = cpTask.getPassword.orNull,
        user = cpTask.getUser.orNull,
        timeout = cpTask.getTimeout.get,
        tendInterval = cpTask.getTendInterval.get,
        writePolicyDefault = wp
      )
    } else {
      ClientPolicy(writePolicyDefault = wp)
    }
  }

  private[this] val hosts: Seq[Host] = tsk.getHosts.map(host => new Host(host.getName, host.getPort))
  private[this] val executor = AsyncCommandExecutor(AsyncClient(hosts: _*))
  private[this] val aerospike = new Aerospike(executor) {
    override protected def namespace: String = tsk.getNamespace
    override protected def setName: String = tsk.getSetName
  }

  private[this] def toJson(a: Any): Json = a match {
    case v: Boolean => v.asJson
    case v: Int => v.asJson
    case v: Long => v.asJson
    case v: Double => v.asJson
    case v: String => v.asJson
    case v: Seq[Any] => Json.array(v.map(x => toJson(x)): _*)
    case v: Map[String, Any] => Json.fromFields(v.map { case (k, va) => (k, toJson(va)) } toSeq)
    case null => Json.empty
    case _ => log.error(s"Unsupported class[${a.getClass}]"); throw new RuntimeException(s"Unsupported class[${a.getClass}]")
  }

  implicit val encoder = Encoder.instance[Any](toJson)

  implicit private[this] val reader: PageReader = new PageReader(schema)

  val createRecords: Page => Process[Task, Seq[Seq[Col]]] = { page =>
    reader.setPage(page)
    val records: ListBuffer[Seq[Col]] = ListBuffer.empty
    while (reader.nextRecord()) {
      val columns = schema.getColumns.toList
      val convs = columns.map(Col.of)
      records += convs
    }
    Process.eval(Task.now(records))
  }

  val toRecords: Seq[Seq[Col]] => Seq[Map[String, Any]] = _ map { row =>
    val rec: MMap[String, Any] = MMap.empty
    row foreach {
      case DoubleColumn(i, n, v) => rec += n -> v
      case LongColumn(i, n, v) => rec += n -> v
      case StringColumn(i, n, v) =>
        if (tsk.getSplitters.isPresent) {
          val sps = tsk.getSplitters.get.toMap
          sps.get(n) match {
            case None => //
              rec += n -> v
            case Some(sp) =>
              val sep = sp.getSeparator
              sp.getElementType match {
                case "long" =>
                  val x = v.split(sep).toSeq.map(s => if (s.isEmpty) "0" else s).map(_.toLong)
                  rec += n -> x
                case "double" =>
                  val x = v.split(sep).toSeq.map(s => if (s.isEmpty) "0" else s).map(_.toDouble)
                  rec += n -> x
                case "string" =>
                  val x = v.split(sep).toSeq
                  rec += n -> x
              }
          }
        } else {
          rec += n -> v
        }
      case BooleanColumn(i, n, v) => rec += n -> v
      case TimestampColumn(i, n, v) => rec += n -> v
      case NullColumn(i, n, t) => // nop
    }
    rec.toMap
  }

  val updater: Sink[Task, Seq[Map[String, Any]]] = sink.lift[Task, Seq[Map[String, Any]]] { records =>
    val latch = new CountDownLatch(records.size)
    val queue = new ConcurrentLinkedQueue[Throwable Xor String]()
    records foreach { record =>
      val keyObj = record.getOrElse(tsk.getKeyName.get, "")
      val deRec = record - tsk.getKeyName.get
      if (tsk.getSingleBinName.isPresent) {
        aerospike.put(keyObj.toString, Map(tsk.getSingleBinName.get() -> deRec)) runAsync {
          case -\/(e) => queue.add(Xor.left(e)); latch.countDown()
          case \/-(r) => queue.add(r); latch.countDown()
        }
      } else {
        aerospike.put(keyObj.toString, deRec) runAsync {
          case -\/(e) => queue.add(Xor.left(e)); latch.countDown()
          case \/-(r) => queue.add(r); latch.countDown()
        }
      }
    }

    latch.await()

    Task.delay {
      queue foreach {
        case Left(e @ PutError(key, cause)) =>
          log.error(e.toString, e)
          failures += key -> cause.getMessage
          failCount.addAndGet(1L)
        case Left(e) =>
          log.error(e.toString, e)
          failures += e.getMessage -> e.getMessage
          failCount.addAndGet(1L)
        case Right(_) =>
          successCount.addAndGet(1L)
      }
    }
  }

  val deleter: Sink[Task, Seq[Map[String, Any]]] = sink.lift[Task, Seq[Map[String, Any]]] { records =>
    val latch = new CountDownLatch(records.size)
    val queue = new ConcurrentLinkedQueue[DeleteError Xor Boolean]()
    records foreach { record =>
      val keyObj = record.getOrElse(tsk.getKeyName.get, "")
      val key = keyObj.toString
      aerospike.delete(key) runAsync {
        case -\/(e) => queue.add(Xor.left(DeleteError(key, e))); latch.countDown()
        case \/-(r) => queue.add(r); latch.countDown()
      }
    }

    latch.await()

    Task.delay {
      queue foreach {
        case Left(DeleteError(key, cause)) =>
          log.error(key, cause)
          failures += key -> cause.getMessage
          failCount.addAndGet(1L)
        case Right(_) =>
          successCount.addAndGet(1L)
      }
    }
  }

  def add(page: Page): Unit = {
    tsk.getCommand match {
      case "put" =>
        createRecords(page).takeWhile(_.nonEmpty).map(toRecords).to(updater).run.run
      case "delete" =>
        createRecords(page).takeWhile(_.nonEmpty).map(toRecords).to(deleter).run.run
    }
  }

  def finish(): Unit = log.info(s"finish ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def close(): Unit = {
    reader.close()
    executor.close
  }

  def abort(): Unit = log.error(s"abort ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def commit: TaskReport = {
    var r = Exec.newTaskReport
    r.set("rans", successCount.longValue() + failCount.longValue())
    r.set("failures", failures.toMap.asJson.pretty(Printer.noSpaces))
    r
  }
}

object ops {

  sealed trait Col

  object Col {
    def of(c: Column)(implicit r: PageReader) =
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
