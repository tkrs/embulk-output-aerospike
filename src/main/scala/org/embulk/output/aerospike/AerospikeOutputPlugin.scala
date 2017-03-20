package org.embulk.output.aerospike

import java.util.{ List => JList, Map => JMap }

import com.google.common.base.Optional
import org.embulk.config._
import org.embulk.spi._

object AerospikeOutputPlugin {

  trait PluginTask extends Task {

    @Config("hosts") def getHosts: JList[AerospikeOutputPlugin.HostTask]

    @Config("command") def getCommand: String

    @Config("namespace") def getNamespace: String

    @Config("set_name") def getSetName: String

    @Config("key_name")
    @ConfigDefault("key") def getKeyName: Optional[String]

    @Config("client_policy")
    @ConfigDefault("null") def getClientPolicy: Optional[AerospikeOutputPlugin.ClientPolicyTask]

    @Config("write_policy")
    @ConfigDefault("null") def getWritePolicy: Optional[AerospikeOutputPlugin.WritePolicyTask]

    @Config("single_bin_name")
    @ConfigDefault("null") def getSingleBinName: Optional[String]

    @Config("splitters")
    @ConfigDefault("null") def getSplitters: Optional[JMap[String, AerospikeOutputPlugin.SplitterTask]]

  }

  trait SplitterTask extends Task {
    @Config("separator")
    @ConfigDefault(",") def getSeparator: String

    @Config("element_type")
    @ConfigDefault("string") def getElementType: String
  }

  trait HostTask extends Task {
    @Config("name") def getName: String

    @Config("port") def getPort: Int
  }

  trait ClientPolicyTask extends Task {
    @Config("user")
    @ConfigDefault("null") def getUser: Optional[String]

    @Config("password")
    @ConfigDefault("null") def getPassword: Optional[String]

    @Config("timeout")
    @ConfigDefault("0") def getTimeout: Optional[Integer]

    @Deprecated
    @Config("max_threads")
    @ConfigDefault("300") def getMaxThreads: Optional[Integer]

    @Config("max_conns_per_node")
    @ConfigDefault("300") def getMaxConnsPerNode: Optional[Integer]

    @Config("max_socket_idle")
    @ConfigDefault("14") def getMaxSocketIdle: Optional[Integer]

    @Config("tend_interval")
    @ConfigDefault("1000") def getTendInterval: Optional[Integer]

    @Config("fail_if_not_connected")
    @ConfigDefault("true") def getFailIfNotConnected: Optional[Boolean]
  }

  trait WritePolicyTask extends Task {
    @Config("generation")
    @ConfigDefault("0") def getGeneration: Optional[Integer]

    @Config("expiration")
    @ConfigDefault("0") def getExpiration: Optional[Integer]

    @Config("max_retries")
    @ConfigDefault("0") def getMaxRetries: Optional[Integer]

    @Config("send_key")
    @ConfigDefault("false") def getSendKey: Optional[Boolean]

    @Config("sleep_between_retries")
    @ConfigDefault("0") def getSleepBetweenRetries: Optional[Integer]
  }

}

class AerospikeOutputPlugin extends OutputPlugin {
  import OutputPlugin._
  import AerospikeOutputPlugin._
  import scala.collection.JavaConversions._

  def transaction(config: ConfigSource, schema: Schema, taskCount: Int, control: Control): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])
    control.run(task.dump).foldRight(Exec.newConfigDiff) { (l, r) => r.merge(l) }
  }

  def resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: Control): ConfigDiff =
    throw new UnsupportedOperationException("aerospike output plugin does not support resuming")

  def open(taskSource: TaskSource, schema: Schema, taskIndex: Int): TransactionalPageOutput =
    new AerospikePageOutput(taskSource, schema, taskIndex)

  override def cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: JList[TaskReport]): Unit = {
  }
}
