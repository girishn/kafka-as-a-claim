FROM python:3.11-slim
WORKDIR /app
RUN pip install confluent-kafka==2.3.0 --no-cache-dir
COPY producer.py .
CMD ["python", "producer.py"]
