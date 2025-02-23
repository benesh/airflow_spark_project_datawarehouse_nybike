FROM  docker.io/bitnami/spark:3.5

USER root
RUN apt-get update && apt-get install -y wget tar scala curl

ENV HADOOP_VERSION=3.4.1

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

RUN tar -xf hadoop-$HADOOP_VERSION.tar.gz -C /opt/bitnami \
    && ln -s /opt/bitnami/hadoop-$HADOOP_VERSION /opt/bitnami/hadoop-$HADOOP_VERSION \
    && rm hadoop-$HADOOP_VERSION.tar.gz

ENV HADOOP_HOME=/opt/bitnami/hadoop-$HADOOP_VERSION 
RUN export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

# RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar -Lo /opt/bitnami/spark/jars/jars/hadoop-common-3.4.1.jar
# RUN curl -s https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.29.38/s3-2.29.38.jar -Lo /opt/bitnami/spark/jars/jars/s3-2.29.38.jar
# RUN curl -s https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.38/bundle-2.29.38.jar -Lo /opt/bitnami/spark/jars/bundle-2.29.38.jar
# RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -Lo /opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar
RUN curl -s https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -Lo /opt/bitnami/spark/jars/postgresql-42.7.4.jar   
RUN pip install notebook boto3 psycopg2-binary pydantic sqlmodel sqlalchemy
