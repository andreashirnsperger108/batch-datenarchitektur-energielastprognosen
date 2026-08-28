ARG SPARK_VERSION=3.5.7
FROM spark:${SPARK_VERSION}-scala2.12-java17-python3-ubuntu

USER root
RUN python3 -m pip install --no-cache-dir \
      fastapi==0.116.1 uvicorn==0.35.0
COPY src/power_pipeline/spark_driver_api.py /opt/pipeline/spark_driver_api.py
RUN chown -R 185:0 /opt/pipeline

USER 185:0
ENV HOME=/tmp
EXPOSE 8001
CMD ["python3", "-m", "uvicorn", "spark_driver_api:app", "--app-dir", "/opt/pipeline", "--host", "0.0.0.0", "--port", "8001", "--no-access-log"]
