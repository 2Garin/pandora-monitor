FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pandora-cas==0.0.16
COPY pandora_monitor.py /app/pandora_monitor.py
USER 65534:65534
CMD ["python", "-u", "/app/pandora_monitor.py"]
