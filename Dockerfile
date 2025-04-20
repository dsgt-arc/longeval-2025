FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk \
    python3 \
    python3-pip \
    python3-venv \
    maven \
    curl \
    wget \
    git \
    vim \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH=$HOME/.local/bin:$PATH

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY pom.xml ./

RUN uv sync

# Copy the rest of the application code
COPY . .
