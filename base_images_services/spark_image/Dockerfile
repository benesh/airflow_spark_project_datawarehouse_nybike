FROM python:3.12.9-bullseye AS spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

## Download spark and hadoop dependencies and install
## ENV variables
ENV SPARK_VERSION="3.5.8"
ENV POETRY_VERSION="1.8.1"

ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV APP_HOME=/pyspark_etl_pipeline
ENV JOBS_HOME=/jobs

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"

ENV PYTHONPATH=${SPARK_HOME}/python/:${PYTHONPATH}
ENV PYSPARK_PYTHON=python3

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*

RUN mkdir -p $HADOOP_HOME && mkdir -p $SPARK_HOME
WORKDIR ${SPARK_HOME}
 
# Download spark
# see resources: https://dlcdn.apache.org/spark/spark-3.5.6/
# filename: spark-3.5.6-bin-hadoop3.tgz 

RUN mkdir -p ${SPARK_HOME}
RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz 
RUN tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1
RUN rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz


# Add spark binaries to shell and enable execution
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" 

# Add a spark config for all nodes
COPY ./base_images_services/spark_image/spark-defaults.conf $SPARK_HOME/conf/

# Add Ivy settings for official Maven repository only
COPY ./base_images_services/spark_image/ivysettings.xml $SPARK_HOME/conf/ivysettings.xml
# COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Spark default config: point to Ivy settings
# RUN echo "spark.jars.ivySettings /opt/spark/conf/ivysettings.xml" >> /opt/spark/conf/spark-defaults.conf
RUN echo "spark_base image built with sucecss"

RUN pip3 install poetry==$POETRY_VERSION

WORKDIR $APP_HOME

COPY project_etl_pipeline/pyproject.toml . 
COPY project_etl_pipeline/poetry.lock .
COPY project_etl_pipeline/src ./src
COPY project_etl_pipeline/test ./test

RUN poetry config virtualenvs.create false 

ENV PYTHONPATH=$PYTHONPATH:/pyspark_etl_pipeline/src

FROM spark-base AS pyspark

# Install python deps
RUN poetry install --with spark
# Copy only dependency files first (to leverage Docker cache)


FROM pyspark AS pyspark-runner


# Optionally install Jupyter
FROM pyspark-runner AS pyspark-jupyter

# Install python deps
RUN poetry install --with sparkmaster
# RUN pip3 install notebook

WORKDIR /

COPY ./base_images_services/spark_image/entrypoint.sh .
RUN chmod u+x entrypoint.sh

ENV JUPYTER_PORT=8889

ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON_OPTS="jupyter lab --no-browser --allow-root --ip=0.0.0.0 --port=${JUPYTER_PORT}"
# RUN mkdir /opt/notebooks
# # --ip=0.0.0.0 - listen all interfaces
# # --port=${JUPYTER_PORT} - listen ip on port 8889
# # --allow-root - to run Jupyter in this container by root user. It is adviced to change the user to non-root.


#ENTRYPOINT ["./entrypoint.sh"]
#CMD [ "bash" ]

# Now go to interactive shell mode
# -$ docker exec -it spark-master /bin/bash 
# then execute
# -$ pyspark

# If Jupyter is installed, you will see an URL: `http://127.0.0.1:8889/?token=...`
# This will open Jupyter web UI in your host machine browser.
# Then go to /warehouse/ and test the installation.
