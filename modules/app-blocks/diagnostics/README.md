# `diagnostics` user guide

## Overview

`diagnostics` is a library that provides a consistent way to collect and expose diagnostic information about a running
JVM application. Its main goal is to collect various operational signals such that a troubleshooter could have all
necessary first-responder information accessible in a single place.

Embedding `diagnostics` into an application enables to collect much needed data for the purposes of supporting the
application without requiring setting up any dedicated monitoring infrastructure components (for
example, [Prometheus](https://prometheus.io/)).

At the moment, `diagnostics` covers the following operational domains (with more to come in near future):

- [metrics](#metrics)
- [thread dumps](#thread-dumps)

Where applicable, data is being gathered in a rolling window manner such that behavioural dynamics of the application
can be observed over time for more precise analysis.

## Usage

Integration into a JVM-based application is straightforward as the following code snippet demonstrates:

```scala
package my.fancy.app

object TheApp {
  def main(args: Array[String]): Unit = {
    com.digitalasset.diagnostics.DiagnosticsSocketServer.start()
    // Run your app and do something useful
    println("Hello, World!")
    Thread.sleep(90_000L)
  }
}
```

<details>
<summary>App execution</summary>

```text
[diagnostics] Initialising diagnostics server with configuration:
Config(
  enabled = true,
  host = "127.0.0.1",
  port = 0,
  dumpPath = None,
  metrics = MetricsConfig(interval = PT10S, bufferSize = 60, tags = ArraySeq()),
  threads = ThreadsConfig(interval = PT1M, bufferSize = 10)
)
[diagnostics] Initialising OpenMetrics storage: samples = 60, labels = Set()
[diagnostics] Starting Micrometer to OpenMetrics bridge: interval = PT10S
[diagnostics] Starting thread dumps collector: interval = PT1M, samples = 10
[diagnostics] Server socket listening on /127.0.0.1:52325
Hello, World!
... after a while ...
[diagnostics] Diagnostics server shutting down now...
[diagnostics] Shutting down Micrometer to OpenMetrics bridge
[diagnostics] Shutting down thread dumps collector
```

</details>

While the app runs, the collected information (a ZIP file) is exposed via a socket channel which can be accessed with
a `netcat`-like tool.

```shell
$ nc 127.0.0.1 52325 > health-dump.zip
$ unzip health-dump.zip
Archive:  health-dump.zip
  inflating: metrics.openmetrics
  inflating: threads-20250307-105606.zip
```

Additionally, `diagnostics` can also write the collected information to a local file system on _graceful_ JVM
termination if configured to do so (see notes [below](#graceful-shutdown) when this should not be relied upon) as
secondary mechanism of exposing the data.

## Configuration

Considering the fact that `diagnostics` is a library and not a standalone application (and represents a cross-cutting
concern at that), it is customary to be configured through consistent environmental means. The table below lists the
available configuration sources with priority decreasing from left to right:

| System property                      | Environment variable                 | Default value | Description                                                                                                                                                                                   |
|--------------------------------------|--------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `da.diagnostics.enabled`             | `DA_DIAGNOSTICS_ENABLED`             | `true`        | Enables/disables diagnostics data collection and exposition                                                                                                                                   |
| `da.diagnostics.host`                | `DA_DIAGNOSTICS_HOST`                | `127.0.0.1`   | Hostname or IP address to use for binding the exposition socket                                                                                                                               |
| `da.diagnostics.port`                | `DA_DIAGNOSTICS_PORT`                | `0`           | Port to use for binding the exposition socket (`0` = [random](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/InetSocketAddress.html#%3Cinit%3E(java.lang.String,int))) |
| `da.diagnostics.dump.path`           | `DA_DIAGNOSTICS_DUMP_PATH`           | `<empty>`     | Directory to write to on graceful shutdown (path needs to be an existing writable directory)                                                                                                  |
| `da.diagnostics.metrics.interval`    | `DA_DIAGNOSTICS_METRICS_INTERVAL`    | `PT10S`       | Metrics collection interval (parsing [rules](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html#parse(java.lang.CharSequence)))                             |
| `da.diagnostics.metrics.buffer.size` | `DA_DIAGNOSTICS_METRICS_BUFFER_SIZE` | `60`          | Quantity of samples to store for each monitored metric **(rolling window)**                                                                                                                   |
| `da.diagnostics.metrics.tags`        | `DA_DIAGNOSTICS_METRICS_TAGS`        | `<empty>`     | Comma-separated list of additional labels to enrich each metric with during exposition (for example, `job=myapp,env=staging,deployed=20250101`)                                               |
| `da.diagnostics.threads.interval`    | `DA_DIAGNOSTICS_THREADS_INTERVAL`    | `PT1M`        | Thread dumps collection interval (parsing [rules](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html#parse(java.lang.CharSequence)))                        |
| `da.diagnostics.threads.buffer.size` | `DA_DIAGNOSTICS_THREADS_BUFFER_SIZE` | `10`          | Quantity of thread dumps to store **(rolling window)**                                                                                                                                        |

> [!TIP]
> It is **NOT** expected that the embedding application would expose these configuration options to the end user as
> command-line arguments. However, it is free to do so or perform any required customisations if required.

> [!IMPORTANT]
> Any misconfiguration will not halt the application startup, instead a warning is issued with precise offending
> coordinate and the default value is used.
> ```text
> [diagnostics] WARN: Error configuring `da.diagnostics.metrics.interval=PT5D`: Text cannot be parsed to a Duration. Defaulting to PT10S.
> ```

## Operational domains

### Metrics

For now, `diagnostics` only integrates with [Micrometer](https://micrometer.io/) as the metrics collection library.
Support for other libraries (for example, [OpenTelemetry SDK](https://github.com/open-telemetry/opentelemetry-java)) can
be added upon request.

The primary benefit of collecting metrics via `diagnostics` is that it provides a rolling window of historical metrics
data which is exposed according to the rules
of [OpenMetrics](https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md) format, which allows
seamless ingestion of this data by a variety of industry standard tools. The snippet below demonstrates consumption of
such file by
Prometheus' [promtool:](https://prometheus.io/docs/prometheus/latest/command-line/promtool/#promtool-tsdb-create-blocks-from-openmetrics)

```shell
$ promtool tsdb create-blocks-from openmetrics /prometheus/dump.openmetrics /prometheus/data
b09776d7fc849b15097c4185516a1786d6e33309023b1dbf6a58d4005a63c52d
BLOCK ULID                  MIN TIME       MAX TIME       DURATION     NUM SAMPLES  NUM CHUNKS   NUM SERIES   SIZE
01JNQ569CGGKZ0X7ZSNH0S45NR  1741162073269  1741165663293  59m50.024s   141921       1185         396          467269
```

#### Memory footprint

Metrics are stored in memory in a ring buffer which is configured through `interval` and `buffer size`, so require a
fixed amount of resources _per metric_. However, an application is free to emit as many metrics as it needs, so the
total required memory becomes dependent on this factor. Empirically, the following calculations of a real-world
application ([PQS](https://github.com/digital-asset/participant-query-store/releases)) can be taken into account when estimating the memory
footprint for your own application:

- number of metrics: 402
- rolling window: 1 hour (360 samples @ 10 second resolution _per metric_)
- total memory footprint: 13.74 MB
- per metric memory footprint: 35 KB

> [!TIP]
> Nothing beats your own testing, so run a test with the desired configuration, take a heap dump and get your numbers.
> Make sure your environments are provisioned with enough resources to accommodate the diagnostics footprint.

### Thread dumps

In order to be able to effectively troubleshoot certain types of anomalies (for example, deadlocks), it is imperative to
have access to application thread dumps. `diagnostics` provides a way to collect thread dumps in 2 fidelity modes,
depending on the presence of any restrictions:

- high-fidelity - equivalent to the output of `jstack <pid>` or `kill -SIGQUIT <pid>` commands, i.e. the most complete
  and detailed information about the threads that is directly consumable by industry standard tools

- low-fidelity - contains enough information to identify the most common issues, but is less detailed and more compact,
  yet still consumable by [VisualVM](https://visualvm.github.io/)'s
  [Thread Dump Analyzer](https://github.com/irockel/tda) plugin

> [!CAUTION]
> **Requirements for high-fidelity mode**
>
> In order to achieve high-fidelity mode, `diagnostics` performs self-attachment to the JVM process. This might present
> a problem if the available JVM installation is stripped down to the minimum
> and `jdk.attach` [module](https://docs.oracle.com/en/java/javase/17/docs/api/jdk.attach/module-summary.html) is not
> present.
>
> A typical example of a restricted JVM would be a Docker container with only JRE installed (for
> example, `eclipse-temurin:17-jre` instead of `eclipse-temurin:17`).
>
> Follow these links to learn how to overcome this limitation:
> - https://adoptium.net/en-GB/blog/2021/10/jlink-to-produce-own-runtime/
> - https://hub.docker.com/_/eclipse-temurin (section: **Creating a JRE using jlink**)
>
> Lastly, the following JVM options need to be set when launching the application:
> ```shell
> $ java -Djdk.attach.allowAttachSelf --add-exports=jdk.attach/sun.tools.attach=ALL-UNNAMED -jar myfancyapp.jar
> ```

`diagnostics` will probe for high-fidelity mode first, but if pre-requisites are not met will fall back to low-fidelity
with helpful messages what's missing. It is **highly recommended** to arrange your production environment for
high-fidelity mode capabilities.

## Logging

`diagnostics` library emits its messages with `[diagnostics]` line prefix:

- `ERROR`/`WARN` into the standard error stream
- all other into the standard output stream

## Graceful shutdown

If configured to do so, `diagnostics` will write the collected information to a local file system on _graceful_ JVM
termination, for example:

```shell
$ kill -SIGINT <pid>
$ kill -SIGTERM <pid>
$ docker container kill --signal=SIGINT <container-id>
$ docker container kill --signal=SIGTERM <container-id>
```

However, be mindful that in case of _ungraceful_ termination (for example, `kill -9 <pid>`), the JVM is not given an
opportunity to run its shutdown hooks due to its abrupt termination. This point is particularly important to consider in
containerised environments where the JVM might be terminated by the orchestrator at any time due to various reasons (for
example, resource limits violation, scaling down, etc).
