FROM python:3.12.11-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid pipeline --create-home --shell /usr/sbin/nologin pipeline

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --upgrade pip==25.2 \
    && python -m pip install .

FROM base AS runtime
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "power_pipeline.api:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

FROM base AS test
COPY tests ./tests
RUN python -m pip install '.[dev]'
USER 10001:10001
CMD ["pytest"]

