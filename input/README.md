# Input

Inputs can be categorized in several dimensions:

- Batch Size (one, small batch, large batch)
- Needs end-to-end acknowledgement? 
- Source Durability (ephemeral, queue, cursor, observer)

## Batch Size

Yes, it probably makes sense to have a separate code path for single events (fits in memory) vs. batches (potentially infinite in size, does not fit in memory) if we can have less overhead in the code path for single events.


TCP listener: one template, one reader
UDP listener: one template, one reader
S3:
Google Sheets: multiple events, zero readers

SQS: multiple events, one batch.

Run() should produce events directly
Extract() should provide deframing/decoding support using options configured on the input
inputs can use Extract()

GELF should always use specific framing, and the author shouldn't need to manually pair it with the right options

## End-to-End (E2E) Acknowledgement

An input plugin can wait for confirmation that all events in a batch were successfully handled (either sent through all outputs or explicitly dropped).

However, end-to-end acknowledgement uses additional memory and CPU cycles, so plugins should only opt in when it's required.

## Categories

There are several categories of inputs:

- Ephemeral. Receive events directly from another system. If we are unavailable, the event is lost.
- Queue. Read events from another system. Events disappear after being read.
- Cursor. Events are stored in another system and have some kind of total order. We can read from any point, so we need to keep track of the position of our cursor.
- Observer. Compare the current state against a previous known state to generate events.

### Ephemeral

- TCP/UDP listener
- HTTP listener
- internally generated

### Queue

- AWS SQS
- POP3
- Redis queue
- AWS S3 (with bucket notifications)
- RabbitMQ

### Cursor

Examples:

- Google Sheets (with a timestamp column)
- RSS
- AWS S3 (without bucket notifications)

### Observer

This is probably best left out of scope.

- Apt repository

## Timing

Most inputs are expected to run continuously, but that's not the only option.

- Continuous. Always running.
- Periodic. Generate events on a schedule (eg. cron)
- Ad-hoc. For example, to re-run a dead letter queue after fixing a filter.
