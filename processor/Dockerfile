FROM apache/spark:3.5.0-python3

USER root
RUN pip install --default-timeout=1000 redis arrow pandas mlflow "packaging<25"

USER 185
