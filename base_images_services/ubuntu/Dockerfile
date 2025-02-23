FROM ubuntu:25.04

# RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
# RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker

RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update \
  # && apt install python3.12 -y \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless\
  && apt-get install -y wget tar
  # && rm -rf /var/lib/apt/lists/* 

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# RUN 

# Download and install Apache Spark
ENV SPARK_VERSION=3.5.4
RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz 
    # && apt install python3.12 -y \
  
    # https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get install -y python3.12


# Install dependencies needed for Python (build-essential for compiling, etc.)
# RUN apt-get install -y build-essential libssl-dev zlib1g-dev \
#     libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
#     libncurses5-dev libncursesw5-dev xz-utils tk-dev

# # Install Python 3.12.8
# RUN wget https://www.python.org/ftp/python/3.12.8/Python-3.12.8.tgz && \
#     tar -xf Python-3.12.8.tgz && \
#     cd Python-3.12.8 && \
#     ./configure --enable-optimizations && \
#     make altinstall

# # Add Python 3.12 to PATH (important!)
# ENV PATH="/usr/local/bin:$PATH"

# # Verify Python 3.12 installation
# RUN python3.12 --version

# # Install pip for Python 3.12
# RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
#     python3.12 get-pip.py

# # Upgrade pip for Python 3.12 (good practice)
# RUN python3.12 -m pip install --upgrade pip


# RUN useradd -ms /bin/bash apprunner
# USER apprunner

# ENTRYPOINT ["echo 'Hello World'; sleep infinity"]
# ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
# CMD [ "sleep infinity" ]
ENTRYPOINT ["/bin/bash", "-c", "echo 'Hello World'; sleep infinity"]
