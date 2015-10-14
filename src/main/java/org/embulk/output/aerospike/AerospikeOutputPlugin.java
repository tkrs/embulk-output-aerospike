package org.embulk.output.aerospike;

import com.google.common.base.Optional;
import org.embulk.config.*;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.util.List;
import java.util.Map;

public class AerospikeOutputPlugin
        implements OutputPlugin
{
    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control) {
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
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, final Schema schema, int taskIndex) {
        return new AerospikePageOutput(taskSource, schema, taskIndex);
    }

    public interface PluginTask
            extends Task {
        @Config("hosts")
        List<HostTask> getHost();

        @Config("command")
        String getCommand();

        @Config("namespace")
        String getNamespace();

        @Config("set_name")
        String getSetName();

        @Config("key_name")
        @ConfigDefault("key")
        Optional<String> getKeyName();

        @Config("client_policy")
        @ConfigDefault("null")
        Optional<ClientPolicyTask> getClientPolicy();

        @Config("write_policy")
        @ConfigDefault("null")
        Optional<WritePolicyTask> getWritePolicy();

        @Config("single_bin_name")
        @ConfigDefault("null")
        Optional<String> getSingleBinName();

        @Config("splitters")
        @ConfigDefault("null")
        Optional<Map<String, SplitterTask>> getSplitters();

        @Config("parallel")
        @ConfigDefault("false")
        Boolean getParallel();

    }

    public interface SplitterTask
            extends Task {

        @Config("separator")
        @ConfigDefault(",")
        String getSeparator();

        @Config("element_type")
        @ConfigDefault("string")
        String getElementType();
    }

    public interface HostTask
            extends Task {

        @Config("name")
        String getName();

        @Config("port")
        int getPort();
    }

    public interface ClientPolicyTask
            extends Task {

        @Config("user")
        @ConfigDefault("null")
        Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("timeout")
        @ConfigDefault("null")
        Optional<Integer> getTimeout();

        @Config("max_threads")
        @ConfigDefault("null")
        Optional<Integer> getMaxThreads();

        @Config("max_socket_idle")
        @ConfigDefault("null")
        Optional<Integer> getMaxSocketIdle();

        @Config("tend_interval")
        @ConfigDefault("null")
        Optional<Integer> getTendInterval();

        @Config("fail_if_not_connected")
        @ConfigDefault("null")
        Optional<Boolean> getFailIfNotConnected();

    }

    public interface WritePolicyTask
            extends Task {

        @Config("generation")
        @ConfigDefault("null")
        Optional<Integer> getGeneration();

        @Config("expiration")
        @ConfigDefault("null")
        Optional<Integer> getExpiration();

        @Config("max_retries")
        @ConfigDefault("null")
        Optional<Integer> getMaxRetries();

        @Config("send_key")
        @ConfigDefault("null")
        Optional<Boolean> getSendKey();

        @Config("sleep_between_retries")
        @ConfigDefault("null")
        Optional<Integer> getSleepBetweenRetries();

    }
}
