FROM  ../base_image_python/Dockerfile

# Set environment variables
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 

# Download and install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar zx -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Add Spark and Java to PATH
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin \
    SPARK_HOME=/opt/spark

# Add Ivy settings for official Maven repository only
COPY ivysettings.xml /opt/spark/conf/ivysettings.xml
# COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Spark default config: point to Ivy settings
RUN echo "spark.jars.ivySettings /opt/spark/conf/ivysettings.xml" >> /opt/spark/conf/spark-defaults.conf

RUN poetry install --with spak