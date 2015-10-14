Embulk::JavaPlugin.register_output(
  "aerospike", "org.embulk.output.aerospike.AerospikeOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
