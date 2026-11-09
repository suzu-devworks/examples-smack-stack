# examples-smack-stack

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fsuzu-devworks%2Fexamples-smack-stack%2Frefs%2Fheads%2Fmain%2Fpyproject.toml)

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

## Example Usage

See the detailed step-by-step instructions in the documentation below.

- [Documentation](docs/README.md)
