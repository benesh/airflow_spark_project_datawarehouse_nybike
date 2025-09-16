FROM ../spark_base/Dockerfile

RUN poetry install --with sparkmaster

# Jupyter config (no password for demo -- DO NOT USE IN PROD)
RUN mkdir -p /root/.jupyter && \
    echo "c.NotebookApp.token = ''" > /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py


# Expose Spark and Jupyter ports
EXPOSE 7077 8080 8888 4044

# Startup script
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["./start.sh"]