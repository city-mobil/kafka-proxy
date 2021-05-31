# Kafka Proxy Rust Forever (kprf)

HTTP Kafka producer-proxy based on librdkafka & warp

### NOTE: This is non-stable alpha version so API and configuration can be changed.

## Inspirations

In CityMobil we heavily rely on data-processing. It is widely used for pricing, statistics collection etc.
Most of this data is processed from php-fpm. 

For producing data to kafka internal librdkafka thread-pool is explicitly created. It is quite heavy operation
for every php-fpm worker.

One of the easiest solutions is to use some external http-proxy. This is where 'kafka-proxy' (or kprf) comes to play.

## Setup

`rustc 1.51.0` is required to build kafka-proxy.

- To build debug version of kafka-proxy:
```shell
make # builds rust-debug version
```

Built debug binary is stored at `target/debug/kprf`

- To build release version of kafka-proxy:
```shell
make build_release
```

Built release binary is stored at `target/release/kprf`

- To run kafka-proxy(release version is recommended to use in production environment):
```shell
target/release/kprf --config=config_example.yaml
```

## QuickStart

Following examples work if kafka-proxy is set up and running.

```
# Try asynchronous producing
curl 'http://127.0.0.1:4242/push' -H 'Content-Type: application/json' -d '{"records": [{"topic": "SOME_TOPIC", "data": "{"\a"\: "\b"\}"}],
"wait_for_send": false}'

# Possible success response:
{"status": "ok", "errors": []}

# Possible erroring response:
{"status": "error", "errors": [{"status": "error", "error": "some_message"}, {"status": "ok", "error":""}]}
```

JSON Fields:
- `records` - describes records for further producing.
- `records[i].topic` - describes some kafka topic for producing.
- `records[i].data` -  describes some string data. Can be JSON, XML or anything.
- `wait_for_send` - describes if http-producer client has to wait for delivery result or not.
If false, message is produced asynchronously. Otherwise, synchronously. Default value is `false`

## Configuration

At this moment, this options from [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) are supported:

- `kafka.brokers` - alias for `bootstrap.servers` from librdkafka. Default value is empty array.
- `kafka.user` - alias for `sasl.username` from librdkafka. Default value is empty string.
- `kafka.password` - alias for `sasl.password` from librdkafka. Default value is empty string.
- `kafka.message_max_bytes` - alias for `message.max.bytes` from librdkafka. Default value is `1 MiB`.
- `kafka.queue_buffering_max_messages` - alias for `queue.buffering.max.messages` from librdkafka. Default value is `100000`.
- `kafka.queue_buffering_max_ms` - alias for `queue.buffering.max.ms` from librdkafka. Default value is `10`.
- `kafka.queue_buffering_max_kbytes` - alias for `queue.buffering.max.kbytes` from librdkafka. Default value is `1048576`.
- `kafka.retries` - alias for `retries` from librdkafka. Default value is `3`.
- `kafka.message_timeout_ms` - alias for `message.timeout.ms` from librdkafka. Default value is `2000`.
- `kafka.request_timeout_ms` - alias for `request.timeout.ms` from librdkafka. Default value is `30000`.
- `kafka.request_required_acks` - alias for `request.required.acks` from librdkafka. Default value is `-1`.
- `http.port` - port for HTTP server for producing messages. Default value is `4242`
- `output_file` - output file for logging. Default value is `/dev/stdout`

### Example configuration
```yaml
kafka:
  brokers:
    - '127.0.0.1:9092'
  request_required_acks: 1
  queue_buffering_max_ms: 20
  queue_buffering_max_kbytes: 2048 # 2 MiB

http:
  port: 4242
  metrics_port: 8088

output_file: "/dev/stdout"
```

## Benchmarks

We managed to get such results with `wrk` on v0.0.1-alpha version with `4 KiB` message size.

External host configuration:
```text
CPU: 2-core Intel Core Processor (Broadwell) 2095 MHz.
RAM: 4 GiB DDR4
Virtualization: VT-x
Hypervisor vendor: KVM.
```

Kafka-proxy configuration:

```yaml
kafka:
  brokers:
    - "some_kafka_broker_1.some_external_server"
    - "some_kafka_broker_2.some_external_server"
    - "some_kafka_broker_3.some_external_server"
    - "some_kafka_broker_4.some_external_server"
    - "some_kafka_broker_5.some_external_server"
  request_required_acks: 1
  queue_buffering_max_ms: 10
  queue_buffering_max_kbytes: 2048 # 2 MiB

http:
  port: 4242
```

- Asynchronous producing
```text
wrk -s wrk.lua -t256 -c256 -d120s 'http://some_external_host:4242/push'
Running 2m test @ http://some_external_host:4242/push
  256 threads and 256 connections

  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    20.72ms   11.53ms 145.52ms   72.16%
    Req/Sec    49.57     21.97     1.02k    87.15%
  864241 requests in 1.14m, 108.79MB read
Requests/sec:  12672.49
Transfer/sec:      1.60MB
```

- Synchronous producing
```text
  256 threads and 256 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    31.41ms   88.92ms   1.01s    96.82%
    Req/Sec    59.35     13.91   111.00     74.87%
  1765717 requests in 2.00m, 222.28MB read
Requests/sec:  14701.92
Transfer/sec:      1.85MB
```

## Metrics

All metrics are available in OpenMetrics(prometheus) format on `:8088` port(non-configurable at v0.0.1-alpha).

At this moment(v0.0.1-alpha) these metrics are available:

- `http_requests_duration` - Histogram of HTTP requests. Label `push/sync` is set when message produced asynchronously, 
otherwise, `push/async` is set. `code` label is set according to HTTP response status.
- `kafka_internal_queue_size` - Gauge of internal kafka-queue size, per topic.
- `kafka_message_send_duration` - Histogram of kafka message duration before delivery result callback is received, per topic.
- `kafka_sent_messages` - Counter of total kafka messages sent, per topic.
- `kafka_errors_count` - Counter of total kafka errors, per topic.

## Further improvements

1. `Write-Ahead-Log`. Write-Ahead-Log can be a good improvement if pattern of usage is asynchronous producing. Client does not know
if his records were successfully sent to kafka. Adding `WAL` can improve durability.

2. Configuration improvement. ENVIRONMENT configuration is not supported yet.

3. Internal librdkafka statistics exporting. A lot of information can be collected from internal librdkafka stats.