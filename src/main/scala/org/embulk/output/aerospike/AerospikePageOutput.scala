package org.embulk.output.aerospike

import java.util.concurrent.atomic.AtomicLong

import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.Aerospike
import cats.data.Xor, Xor._
import io.circe._, io.circe.syntax._
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.spi._

import scala.collection.mutable.{ Map => MMap }
import scala.collection.JavaConversions._

import scalaz.{ \/-, -\/ }

class AerospikePageOutput(taskSource: TaskSource, schema: Schema, taskIndex: Int) extends TransactionalPageOutput {

  import AerospikeOutputPlugin._

  private[this] val log = Exec.getLogger(classOf[AerospikePageOutput])
  private[this] val tsk = taskSource.loadTask(classOf[AerospikeOutputPlugin.PluginTask])
  private[this] val successCount = new AtomicLong
  private[this] val failCount = new AtomicLong

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
  private[this] val reader: PageReader = new PageReader(schema)

  private[this] def toJson(a: Any): Json = a match {
    case v: Boolean => v.asJson
    case v: Int => v.asJson
    case v: Long => v.asJson
    case v: Double => v.asJson
    case v: String => v.asJson
    case v: Seq[Any] => Json.array(v.map(x => toJson(x)): _*)
    case v: Map[String, Any] => Json.fromFields(v.map { case (k, va) => (k, toJson(va)) } toSeq)
    case _ => ???
  }

  implicit val encoder = Encoder.instance[Any](toJson)

  def add(page: Page) {
    reader.setPage(page)
    while (reader.nextRecord()) {
      val schema = reader.getSchema
      val record: MMap[String, Any] = MMap.empty
      schema.getColumns.foreach { col =>
        val name = col.getName
        if (!(reader isNull col)) {
          col.getType.getName match {
            case "long" =>
              record += name -> reader.getLong(col)
            case "double" =>
              record += name -> reader.getDouble(col)
            case "timestamp" =>
              record += name -> reader.getTimestamp(col).toEpochMilli
            case "boolean" =>
              record += name -> reader.getBoolean(col)
            case "string" =>
              val cv = reader.getString(col)
              if (tsk.getSplitters.isPresent) {
                val sps = tsk.getSplitters.get.toMap
                sps.get(name) match {
                  case None => //
                    record += name -> cv
                  case Some(v) =>
                    val sep = v.getSeparator
                    v.getElementType match {
                      case "long" =>
                        val x = cv.split(sep).map(s => if (s.isEmpty) "0" else s).map(_.toLong)
                        record += name -> x
                      case "double" =>
                        val x = cv.split(sep).map(s => if (s.isEmpty) "0" else s).map(_.toDouble)
                        record += name -> x
                      case "string" =>
                        val x = cv.split(sep)
                        record += name -> x
                    }
                }
              } else {
                record += name -> cv
              }
            case typ => log.error(typ + "[?]")
          }
      }
      val keyObj = record.getOrElse(tsk.getKeyName.get, "")
      record -= tsk.getKeyName.get
      tsk.getCommand match {
        case "put" =>
          val t =
            if (tsk.getSingleBinName.isPresent)
              aerospike.put(keyObj.toString, Map(tsk.getSingleBinName.get() -> record.toMap))
            else
              aerospike.put(keyObj.toString, record.toMap)
          t runAsync {
            case -\/(e) => log.error(e.toString, e.getCause)
            case \/-(x) => x match {
              case Left(ex) =>
                failCount.addAndGet(1L)
                log.error(ex.toString, ex.getCause)
              case Right(v) =>
                successCount.addAndGet(1L)
                log.debug(v.toString)
            }
          }
        case "delete" =>
          val t = aerospike.delete(keyObj.toString)
          t runAsync {
            case -\/(e) => log.error(e.toString, e.getCause)
            case \/-(x) => x match {
              case Left(ex) =>
                failCount.addAndGet(1L)
                log.error(ex.toString, ex.getCause)
              case Right(v) =>
                successCount.addAndGet(1L)
                log.debug(v.toString)
            }
          }
      }
    }
  }

  def finish(): Unit = log.info(s"finish ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def close(): Unit = {
    reader.close()
    executor.close
  }

  def abort(): Unit = log.error(s"abort ${tsk.getCommand} ok[${successCount.longValue}] ng[${failCount.longValue()}]")

  def commit: TaskReport = Exec.newTaskReport
}
