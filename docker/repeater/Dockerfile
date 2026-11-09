FROM ghcr.io/astral-sh/uv:latest AS uv
FROM python:3.12-slim

COPY --from=uv /uv /uvx /bin/
WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

RUN set -x \
    && uv venv \
    && uv pip install confluent-kafka \
    ;

EXPOSE 5005

CMD ["uv", "run", "python"]
