# Aerospike output plugin for Embulk

[![Gem Version](https://badge.fury.io/rb/embulk-output-aerospike.svg)](https://badge.fury.io/rb/embulk-output-aerospike)

Aerospike output plugins for Embulk loads records to databases using AerospikeJavaClient.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **hosts**: (list, required)
  - **name**: hostname (string, required)
  - **port**: port number (int, required)
- **command**: aerospike command(now supported put and delete only) (string, required)
- **namespace**: destination namespace (string, required)
- **set_name**: destination set name (string, required)
- **key_name**: corresponding column name for create destination key. specified column will be excluded from destinations. (hash, default: `key`)
- **client_policy**: (hash, default: `conform to aerospike`)
  - **user**: user name (string, default: `null`)
  - **password**: user password (string, default: `null`)
  - **timeout**: command timeout (int, default: `conform to aerospike`)
  - **max_threads**: max thread numbers (int, default: `conform to aerospike`)
  - **max_socket_idle**: max socket idel numbers (int, default: `conform to aerospike`)
  - **tend_interval**: tend interval numbers (int, default: `conform to aerospike`)
  - **fail_if_not_connected**: fail if not connected (boolean, default: `conform to aerospike`)
- **write_policy**: (hash, default: `conform to aerospike`)
  - **generation**: generation (string, default: `conform to aerospike`)
  - **expiration**: expiration time (int, default: `conform to aerospike`)
  - **max_retries**: max retry numbers (int, default: `conform to aerospike`)
  - **send_key**: send real key (int, default: `conform to aerospike`)
  - **sleep_between_retries**: sleepp between retry numbers (int, default: `conform to aerospike`)
- **single_bin_name**: bin name (string, default: `null`)
- **splitters**: key is column_name (hash, required)
  - **separator**: regexp for splitting separator (string, default: `,`)
  - **element_type**: to type of conversions for each elements. now supported type is string, long and double (string, default: `string`)
- **parallel**: use parallel execute (boolean, default: `false`)

## Example

### single bin mode

```yaml
out:
  type: aerospike
  hosts:
  - {name: '192.168.99.100', port: 3000}
  command: put
  namespace: test
  set_name: set
  single_bin_name: record
  parallel: true
  splitters:
    column1: {separator: '\.*', element_type: string}
    column2: {separator: '\t', element_type: long}

```

### multi bin mode

```yaml
out:
  type: aerospike
  hosts:
  - {name: '192.168.99.100', port: 3000}
  command: put
  namespace: test
  set_name: set
  key_name: column0
  client_policy:
    max_retries: 3
  write_policy:
    generation: 0
    expiration: 64000
    send_key: true
  parallel: true
```



## Build

```sh
./gradlew gem  # -t to watch change of files and rebuild continuously
```
