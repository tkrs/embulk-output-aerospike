# Aerospike output plugin for Embulk

[![Gem Version](https://badge.fury.io/rb/embulk-output-aerospike.svg)](https://badge.fury.io/rb/embulk-output-aerospike)

Aerospike output plugins for Embulk loads records to databases using [aerospiker](https://github.com/tkrs/aerospiker).

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
  - **max_conns_per_node**: max connections allowed per server node (int, default: `conform to aerospike`)
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
```



## Build

```sh
./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Run example

First, start the aerospike-server

```sh
docker run --rm -ti --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server
```

Then, run embulk with [example config](https://github.com/tkrs/embulk-output-aerospike/blob/master/example/config.yml).


```sh
λ embulk -J-O -R--dev run -I lib example/config.yml
2017-11-30 23:49:24.598 +0900: Embulk v0.8.30
2017-11-30 23:49:38.484 +0900 [INFO] (0001:transaction): Loaded plugin embulk/output/aerospike from a load path
2017-11-30 23:49:38.503 +0900 [INFO] (0001:transaction): Listing local files at directory 'example' filtering filename by prefix 'sample.csv'
2017-11-30 23:49:38.505 +0900 [INFO] (0001:transaction): "follow_symlinks" is set false. Note that symbolic links to directories are skipped.
2017-11-30 23:49:38.508 +0900 [INFO] (0001:transaction): Loading files [example/sample.csv]
2017-11-30 23:49:38.544 +0900 [INFO] (0001:transaction): Using local thread executor with max_threads=8 / output tasks 4 = input tasks 1 * 4
2017-11-30 23:49:38.567 +0900 [INFO] (0001:transaction): {done:  0 / 1, running: 0}
2017-11-30 23:49:39.134 +0900 [INFO] (0013:task-0000): finish put ok[6] ng[0]
2017-11-30 23:49:39.135 +0900 [INFO] (0013:task-0000): finish put ok[0] ng[0]
2017-11-30 23:49:39.135 +0900 [INFO] (0013:task-0000): finish put ok[0] ng[0]
2017-11-30 23:49:39.135 +0900 [INFO] (0013:task-0000): finish put ok[0] ng[0]
2017-11-30 23:49:39.278 +0900 [INFO] (0001:transaction): {done:  1 / 1, running: 0}
2017-11-30 23:49:39.284 +0900 [INFO] (main): Committed.
2017-11-30 23:49:39.284 +0900 [INFO] (main): Next config diff: {"in":{"last_path":"example/sample.csv"},"out":{"rans":6,"failures":"{}"}}
```

Let's check it.

```sh
docker exec -it aerospike aql -c "select * from test"
+---------------------------------------+-----+
| user_name                             | age |
+---------------------------------------+-----+
| LIST('["Bomani", "Archaman"]')        | 20  |
| LIST('["Ritsuka", "Fujimura"]')       | 30  |
| LIST('["Fou"]')                       | 999 |
| LIST('["Mash", "Kyrielight"]')        | 20  |
| LIST('["Olgamally", "Animusphere"]')  | 10  |
| LIST('["Lev", "Lainur"]')             | 45  |
+---------------------------------------+-----+
6 rows in set (0.167 secs)
```
