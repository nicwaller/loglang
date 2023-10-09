# Input

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
