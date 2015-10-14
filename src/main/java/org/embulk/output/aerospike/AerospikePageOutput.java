package org.embulk.output.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.type.Type;
import org.jruby.ir.Tuple;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AerospikePageOutput implements TransactionalPageOutput {

    private final Logger log = Exec.getLogger(AerospikePageOutput.class);

    public AerospikePageOutput(TaskSource taskSource, final Schema schema, int taskIndex) {
        reader = new PageReader(schema);
        task = taskSource.loadTask(AerospikeOutputPlugin.PluginTask.class);
        List<Host> hosts = task.getHost().stream()
                .map(host -> new Host(host.getName(), host.getPort()))
                .collect(Collectors.toList());

        AsyncClientPolicy policy = new AsyncClientPolicy();
        if (task.getClientPolicy().isPresent()) {
            AerospikeOutputPlugin.ClientPolicyTask cpTask = task.getClientPolicy().get();
            policy.failIfNotConnected = cpTask.getFailIfNotConnected().or(policy.failIfNotConnected);
            policy.maxThreads = cpTask.getMaxThreads().or(policy.maxThreads);
            policy.maxSocketIdle = cpTask.getMaxSocketIdle().or(policy.maxSocketIdle);
            policy.password = cpTask.getPassword().orNull();
            policy.user = cpTask.getUser().orNull();
            policy.timeout = cpTask.getTimeout().or(policy.timeout);
            policy.tendInterval = cpTask.getTendInterval().or(policy.tendInterval);
        }

        WritePolicy wp = new WritePolicy();
        if (task.getWritePolicy().isPresent()) {
            AerospikeOutputPlugin.WritePolicyTask wpTask = task.getWritePolicy().get();
            wp.sendKey = wpTask.getSendKey().or(wp.sendKey);
            wp.expiration = wpTask.getExpiration().or(wp.expiration);
            wp.maxRetries = wpTask.getMaxRetries().or(wp.maxRetries);
            wp.generation = wpTask.getGeneration().or(wp.generation);
            wp.sleepBetweenRetries = wpTask.getSleepBetweenRetries().or(wp.sleepBetweenRetries);
        }

        policy.asyncWritePolicyDefault = wp;
        aerospike = new AsyncClient(policy, hosts.toArray(new Host[hosts.size()]));
    }

    private final AerospikeOutputPlugin.PluginTask task;

    private final AtomicLong counter = new AtomicLong();

    private final AsyncClient aerospike;

    private final PageReader reader;

    @Override
    public void add(Page page) {

        if (!aerospike.isConnected()) {
            System.out.println("not connected");
            return;
        }

        reader.setPage(page);

        Iterator<Tuple<Key, Map<String, Object>>> it = new Iterator<Tuple<Key, Map<String, Object>>>() {
            @Override public boolean hasNext() { return reader.nextRecord(); }
            @Override public Tuple<Key, Map<String, Object>> next() {
                Schema sc = reader.getSchema();
                Map<String, Object> bins = new HashMap<>();
                Object keyObj = "";

                for (Column column : sc.getColumns()) {

                    int index = column.getIndex();
                    String name = column.getName();

                    int keyIndex = task.getKeyIndex().get();

                    Type type = column.getType();
                    switch (type.getName()) {
                        case "string":
                            if (!reader.isNull(column)) {
                                String value = reader.getString(column);
                                if (index == keyIndex) keyObj = value;
                                bins.put(name, value);
                            }
                            break;
                        case "long":
                            if (!reader.isNull(column)) {
                                Long value = reader.getLong(column);
                                if (index == keyIndex) keyObj = value;
                                bins.put(name, value);
                            }
                            break;
                        case "double":
                            if (!reader.isNull(column)) {
                                Double value = reader.getDouble(column);
                                if (index == keyIndex) keyObj = value;
                                bins.put(name, value);
                            }
                            break;
                        case "boolean":
                            if (!reader.isNull(column)) {
                                Boolean value = reader.getBoolean(column);
                                if (index == keyIndex) keyObj = value;
                                bins.put(name, value);
                            }
                            break;
                        case "timestamp":
                            if (!reader.isNull(column)) {
                                Long value = reader.getTimestamp(column).toEpochMilli();
                                if (index == keyIndex) keyObj = value;
                                bins.put(name, value);
                            }
                        default:
                            break;
                    }
                }


                if (log.isDebugEnabled()) log.debug(keyObj.toString());
                Key key = new Key(task.getNamespace(), task.getSetName(), Value.get(keyObj));
                return new Tuple<>(key, bins);
            }
        };

        Spliterator<Tuple<Key, Map<String, Object>>> spliterator = Spliterators.spliteratorUnknownSize(it, Spliterator.IMMUTABLE);

        Stream<Tuple<Key, Map<String, Object>>> stream = StreamSupport.stream(spliterator, task.getParallel());

        Consumer<Tuple<Key, List<Bin>>> action;
        switch (task.getCommand()) {
            case "put":
                action = rec -> aerospike.put(null, new WriteListener() {
                    @Override public void onSuccess(Key key) { counter.addAndGet(1L); }
                    @Override public void onFailure(AerospikeException e) { log.error(e.getMessage(), e); }
                }, rec.a, rec.b.toArray(new Bin[rec.b.size()]));
                break;
            case "delete":
                action = rec -> aerospike.delete(null, new DeleteListener() {
                    @Override public void onSuccess(Key key, boolean existed) { counter.addAndGet(1L); }
                    @Override public void onFailure(AerospikeException e) { log.error(e.getMessage(), e); }
                }, rec.a);
                break;
            default:
                return;
        }

        stream.map(t -> {
            List<Bin> bins = new ArrayList<>();
            if (task.getSingleBinName().isPresent())
                bins.add(new Bin(task.getSingleBinName().get(), t.b));
            else
                t.b.entrySet().forEach(rec -> bins.add(new Bin(rec.getKey(), rec.getValue())));
            return new Tuple<>(t.a, bins);
        }).forEach(action);
    }

    @Override
    public void finish() {
        log.info("finish %s [%l]", task.getCommand(), counter.longValue());
    }

    @Override
    public void close() {
        reader.close();
        aerospike.close();
    }

    @Override
    public void abort() {
        log.warn("abort");
    }

    @Override
    public TaskReport commit() {
        return Exec.newTaskReport();
    }

}
