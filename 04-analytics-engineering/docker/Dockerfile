# Use slim Python base
FROM python:3.11.2-slim-bullseye

# Install system dependencies
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    libpq-dev \
    make \
    openssh-client \
    software-properties-common \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set env vars for consistent encoding
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Upgrade pip and install dbt-postgres
RUN python -m pip install --upgrade pip setuptools wheel \
  && pip install --no-cache-dir \
    dbt-core \
    dbt-postgres

# Set working directory and default entrypoint
WORKDIR /usr/app
ENTRYPOINT ["dbt"]