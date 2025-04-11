# Base image with Java 17 and Python 3.12.9
FROM openjdk:17-jdk-slim

# Set environment variables
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    PYTHON_VERSION=3.12.9 \
    SCALA_VERSION=3.3.1 \ 
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=jupyter \
    PYSPARK_DRIVER_PYTHON_OPTS="lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser"

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libsqlite3-dev \
    libreadline-dev \
    libffi-dev \
    libbz2-dev \
    git \
    procps \  
    net-tools \ 
    && rm -rf /var/lib/apt/lists/*

# Install Python using pyenv
RUN curl https://pyenv.run | bash
ENV PATH="/root/.pyenv/bin:/root/.pyenv/shims:${PATH}"
RUN pyenv install ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}

# Install pip and required Python packages
RUN pip install --upgrade pip && \
    pip install jupyterlab pyspark==${SPARK_VERSION} findspark

# Create a directory for notebooks
RUN mkdir -p /home/jovyan/work

WORKDIR /home/jovyan

# Download and configure Apache Spark
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark \
    PATH=$SPARK_HOME/bin:$PATH

# Install Scala 3.x
RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs \
&& chmod +x cs \
# && mv cs /usr/bin \
&& ./cs setup -y

# Add Scala to the PATH
# ENV SCALA_HOME=/opt/scala \
#     PATH=$SCALA_HOME/bin:$PATH


# Copy startup script into the container
COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

# Expose ports for Jupyter Lab and Spark UI
EXPOSE 8888 4040 7077 8080

# Start Jupyter Lab
# CMD ["/start-spark.sh"]

ENTRYPOINT ["/start-spark.sh"]
CMD ["source ~/.profile"]