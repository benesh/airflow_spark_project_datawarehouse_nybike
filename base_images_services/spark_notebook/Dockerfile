FROM python:3.11-slim

# Install Java, curl, wget, and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk wget curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download and install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar zx -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Add Spark and Java to PATH
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# Install JupyterLab and PySpark
RUN pip install --upgrade pip && \
    pip install pyspark==${SPARK_VERSION} jupyterlab pydantic sqlmodel 

ENV SCALA_VERSION=2.12.19
RUN wget -q https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb && \
    apt-get update && \
    apt-get install -y ./scala-${SCALA_VERSION}.deb && \
    rm scala-${SCALA_VERSION}.deb && \
    rm -rf /var/lib/apt/lists/*


RUN apt-get update && apt-get install -y procps    

# Jupyter config (no password for demo -- DO NOT USE IN PROD)
RUN mkdir -p /root/.jupyter && \
    echo "c.NotebookApp.token = ''" > /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py

# Add Ivy settings for official Maven repository only
COPY ivysettings.xml /opt/spark/conf/ivysettings.xml
# COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Spark default config: point to Ivy settings
RUN echo "spark.jars.ivySettings /opt/spark/conf/ivysettings.xml" >> /opt/spark/conf/spark-defaults.conf

# Expose Spark and Jupyter ports
EXPOSE 7077 8080 8888 4044

# Startup script
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]