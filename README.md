# examples-smack-stack

Since I was already working with Cassandra, I decided to take the opportunity to explore
the whole thing—and that’s how this project came about.

This is a trial version with an energy-saving configuration; it is far from a production-ready setup,
but it serves to give you a feel for how it works.

## SMACK Stack　is

This term is an acronym derived from a collection of open-source software components used together
for real-time processing and big data analysis.

- **Spark**: A unified analytics engine for big data processing, with built-in modules for streaming,
    SQL, machine learning, and graph processing.
- **Mesos**: A cluster manager that provides efficient resource isolation
    and sharing across distributed applications or frameworks.
    In this instance, Docker Compose is used as a substitute.
- **Akka**: A toolkit for building highly concurrent, distributed, and resilient message-driven applications.
    I am not using it.
- **Cassandra**: A highly scalable NoSQL database.
- **Kafka**: A distributed event streaming platform capable of handling high throughput and low-latency data feeds.
