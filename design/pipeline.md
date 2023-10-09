# Pipeline

Inputs should produce Events. Sometimes they need a bit of help along the way:

- `Framing` converts a byte stream (`io.Reader`) into a sequence of byte streams. Framing includes compression, encryption, and authentication if applicable.
- `Codec` converts a byte stream into a single event.
