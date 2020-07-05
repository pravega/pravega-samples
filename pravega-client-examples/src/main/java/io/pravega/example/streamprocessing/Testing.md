
# Ungraceful shutdown

- Start multiple JVMs for processors
- Write events
- Read processed events
- Kill -9 JVMs
- Start new JVMs for processors
- Validate that all events are processed at least once.
- Validate that, if an idempotent writer were used (IdempotentEventWriter), all events would be processed exactly once.
- Ordering guarantee: maintains per-routing key ordering, with rewind.

# Graceful shutdown

- Same as above but use normal kill.
- Validate exactly once

# Temporary network partition

- Must block access from JVM to Pravega.
- Run in Docker and use iptables to block IPs?
- Must create Docker image with this application.