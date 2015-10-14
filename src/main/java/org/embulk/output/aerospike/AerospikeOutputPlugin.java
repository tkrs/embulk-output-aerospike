package org.embulk.output.aerospike;

import java.util.*;

import com.google.common.base.Optional;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;

public class AerospikeOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("hosts")
        public List<HostTask> getHost();

        @Config("command")
        public String getCommand();

        @Config("namespace")
        public String getNamespace();

        @Config("set_name")
        public String getSetName();

        @Config("key_index")
        @ConfigDefault("0")
        public Optional<Integer> getKeyIndex();

        @Config("client_policy")
        @ConfigDefault("null")
        public Optional<ClientPolicyTask> getClientPolicy();

        @Config("write_policy")
        @ConfigDefault("null")
        public Optional<WritePolicyTask> getWritePolicy();

        @Config("single_bin_name")
        @ConfigDefault("null")
        public Optional<String> getSingleBinName();

        @Config("parallel")
        @ConfigDefault("false")
        public Boolean getParallel();

    }

    public interface HostTask
            extends Task {

        @Config("name")
        public String getName();

        @Config("port")
        public int getPort();
    }

    public interface ClientPolicyTask
            extends Task {

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("timeout")
        @ConfigDefault("null")
        public Optional<Integer> getTimeout();

        @Config("max_threads")
        @ConfigDefault("null")
        public Optional<Integer> getMaxThreads();

        @Config("max_socket_idle")
        @ConfigDefault("null")
        public Optional<Integer> getMaxSocketIdle();

        @Config("tend_interval")
        @ConfigDefault("null")
        public Optional<Integer> getTendInterval();

        @Config("fail_if_not_connected")
        @ConfigDefault("null")
        public Optional<Boolean> getFailIfNotConnected();

    }

    public interface WritePolicyTask
            extends Task {

        @Config("generation")
        @ConfigDefault("null")
        public Optional<Integer> getGeneration();

        @Config("expiration")
        @ConfigDefault("null")
        public Optional<Integer> getExpiration();

        @Config("max_retries")
        @ConfigDefault("null")
        public Optional<Integer> getMaxRetries();

        @Config("send_key")
        @ConfigDefault("null")
        public Optional<Boolean> getSendKey();

        @Config("sleep_between_retries")
        @ConfigDefault("null")
        public Optional<Integer> getSleepBetweenRetries();

    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("aerospike output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, final Schema schema, int taskIndex)
    {
        return new AerospikePageOutput(taskSource, schema, taskIndex);
    }
}
