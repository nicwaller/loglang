# Framing

There's a few different ways to mark discrete records in a stream of bytes.

Some framing may also introduce checksums or error correcting codes (ECC).

## Delimiter-based

A delimiter is some kind of sequence that occurs between records.

- Newline-delimited. This is the most common type, and is especially used with minified JSON in a format called NDJSON (newline-delimited JSON) or JSON-Lines.
- Multiline. Java stack traces and Go panic traces occur over multiple lines. It's desirable to group these lines together into a single timestamped event.
- Multipart/Mixed uses a long `boundary` string. This kind of framing is mostly used for email attachments.
- Mul
- Regular Expressions can be used to pick records out of a stream.
- [COBS](https://en.m.wikipedia.org/wiki/Consistent_Overhead_Byte_Stuffing)

## Header-based

Header-based framing avoids delimiters by putting the length of the frame at the beginning of the frame. The length of the frame is encoded in a sequence of bytes at the beginning of the frame. Often 4 bytes, sometimes less. Also known as _Pascal Strings_.

## Fixed Width

Every record is the same size. This has the nice characteristic where it's easy to jump to any point in a file and be sure you're at the start of a record.

Wen packing multiple Protobuf records into a single file, vector-based framing is the [usual](https://seb-nyberg.medium.com/length-delimited-protobuf-streams-a39ebc4a4565) choice. 
