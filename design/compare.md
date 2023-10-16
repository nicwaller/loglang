# Comparison

There are several tools for event processing. How do they compare?

# Event Processors

## Fluent Bit

https://fluentbit.io

## Logstash (by Elastic)

https://www.elastic.co/logstash

## Vector (by Datadog)

https://vector.dev

## Amazon EventBridge Pipes

https://aws.amazon.com/eventbridge/pipes/

## Seq

https://docs.datalust.co/v2023.2/docs

## rsyslogd

https://www.rsyslog.com

# Logs-to-Metrics

## mtail

Are you running an application that only emits logs, but what you really want is metrics? For example, maybe you're running a webserver and you want metrics about http response codes.

Well, some engineers at Google had the same problem and they solved it in traditional Google fashion: by inventing a new programming language called mtail specifically tuned to emit counter/gauge/histogram metrics.

It reads very much like awk:

```
counter lines_total

/$/ {
lines_total++
}
```

By default it listens on :3903 and provides a Prometheus-compatible /metrics endpoint. It can also emit metrics using the graphite plaintext protocol for people who prefer that.
