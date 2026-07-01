FROM python:3.11-slim
WORKDIR /app
RUN pip install confluent-kafka==2.3.0 certifi --no-cache-dir
COPY consumer.py .
CMD ["python", "consumer.py"]
