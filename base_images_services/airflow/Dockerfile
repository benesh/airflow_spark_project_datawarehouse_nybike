# FROM apache/airflow:slim-2.9.2-python3.12
FROM  ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.3}
# COPY pyproject.toml .

RUN pip install apache-airflow-providers-apache-spark==3.0.0 pyspark==3.5.5 sqlmodel


USER 0

# # Install Poetry (if not already installed on the base image)
# RUN apt-get update && apt-get install -y --no-install-recommends \

# python3-pip  # Assuming Debian-based system

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


# # Set JAVA_HOME environment variable
# #ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64


# # The installer requires curl (and certificates) to download the release archive
# RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# # Download the latest installer
# ADD https://astral.sh/uv/install.sh /uv-installer.sh

# # Run the installer then remove it
# RUN sh /uv-installer.sh && rm /uv-installer.sh

# # Ensure the installed binary is on the `PATH`
# ENV PATH="/root/.local/bin/:$PATH"

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark

# # Install Poetry
# # RUN pip install poetry

# # Install dependencies with Poetry (excluding test dependencies)
# # RUN poetry config virtualenvs.create false  # Disable virtualenv creation within Docker
# # RUN poetry install --no-dev  # Install dependencies excluding development packages

# # Copy the project into the image
# ADD . /app

# # Sync the project into a new environment, using the frozen lockfile
# WORKDIR /app
# RUN uv sync --frozen
