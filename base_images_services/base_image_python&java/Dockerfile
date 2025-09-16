FROM python:3.12-slim

ENV POETRY_VERSION=1.8.1

# Install Java, curl, wget, and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk wget curl ca-certificates && \

ENV SCALA_VERSION=2.12.19
RUN wget -q https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb && \
    apt-get update && \
    apt-get install -y ./scala-${SCALA_VERSION}.deb && \
    rm scala-${SCALA_VERSION}.deb && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update && apt-get install -y procps    

# Install Poetry
RUN pip install poetry==$POETRY_VERSION
ENV PATH="/root/.local/bin:$PATH"


# Copy only dependency files first (to leverage Docker cache)
COPY ../../pyproject.toml ../../poetry.lock ./

# Install dependencies (without creating a virtualenv)
# RUN poetry config virtualenvs.create false \
    # && poetry install --no-interaction --no-ansi

RUN poetry config virtualenvs.create false 
