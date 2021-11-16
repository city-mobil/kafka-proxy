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
- `http.metrics_port` - port for HTTP server for metrics. Default value is `8088`
- `output_file` - output file for logging. Default value is `/dev/stdout`
- `ratelimit.enabled` - enable or disable rate limits. Default value is `false`
- `ratelimit.rules` - rules for rate limits. Default value is `[]`

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

ratelimit: # ratelimit settings.
  enabled: true # enables or disables ratelimiting.
  rules: # defines list of rules for concrete topics.
    - topic_name: "some_topic_name" # concrete topic name.
      max_requests_per_minute: 42 # maximum requests per minute allowed for concrete topic.
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

At this moment(v0.1.0) these metrics are available:

- `http_requests_duration` - Histogram of HTTP requests. Label `push/sync` is set when message produced asynchronously, 
otherwise, `push/async` is set. `code` label is set according to HTTP response status.
- `kafka_internal_queue_size` - Gauge of internal kafka-queue size, per topic.
- `kafka_message_send_duration` - Histogram of kafka message duration before delivery result callback is received, per topic.
- `kafka_sent_messages` - Counter of total kafka messages sent, per topic.
- `kafka_errors_count` - Counter of total kafka errors, per topic.
- `ratelimit_messages_count` - Counter of total ratelimited messages, per topic.

Kafka librdkafka metrics:
- `kafka_producer_reply_queue_size` - Operations (callbacks, events, etc.) waiting in queue.
- `kafka_producer_current_messages_in_queue` - Current number of messages in producer queues.
- `kafka_producer_current_messages_in_queue_bytes` - Current total size of messages in producer queues.
- `kafka_producer_total_requests_count` - Total number of requests sent to brokers
- `kafka_producer_total_bytes_sent` - Total number of bytes transmitted to brokers
- `kafka_producer_total_responses_received` - Total number of responses received from brokers
- `kafka_producer_total_bytes_received` - Total number of bytes received from brokers
- `kafka_producer_total_messages_sent` - Total number of messages transmitted (produced) to brokers
- `kafka_producer_total_messages_bytes_sent` - Total number of bytes transmitted (produced) to brokers
- `kafka_producer_metadata_cache_topics_count` - Number of topics in the metadata cache
- `kafka_producer_broker_state` - Broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY, AUTH_HANDSHAKE, UP, UPDATE).
- `kafka_producer_broker_state_age` - The time since the last broker state change, in microseconds
- `kafka_producer_broker_outbuf_count` - Number of requests awaiting transmission to the broker
- `kafka_producer_broker_outbuf_msg_count` - Number of messages awaiting transmission to the broker
- `kafka_producer_broker_waitresp_count` - Number of requests in-flight to the broker that are awaiting response
- `kafka_producer_broker_waitresp_msg_count` - Number of messages awaiting transmission to the broker
- `kafka_producer_broker_requests_sent` - Total number of requests sent to the broker
- `kafka_producer_broker_requests_sent_bytes` - Total number of bytes sent to the broker
- `kafka_producer_broker_transmission_errors` - Total number of transmission errors
- `kafka_producer_broker_request_retries` - Total number of request retries
- `kafka_producer_request_timeouts` - Total number of requests that timed out
- `kafka_producer_broker_responses_count` - Total number of responses received from the broker
- `kafka_producer_broker_bytes_received` - Total number of bytes received from the broker
- `kafka_producer_broker_errors_count` - Total number of received errors
- `kafka_producer_topic_metadata_age` - The age of the client's metadata for this topic, in milliseconds
- `kafka_producer_topic_batchsize_avg` - Rolling window statistics for batch sizes, in bytes
- `kafka_producer_topic_batchcount_avg` - Rolling window statistics for batch message counts

## Further improvements

1. `Write-Ahead-Log`. Write-Ahead-Log can be a good improvement if pattern of usage is asynchronous producing. Client does not know
if his records were successfully sent to kafka. Adding `WAL` can improve durability.