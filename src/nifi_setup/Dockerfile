FROM python:3.11-slim

WORKDIR /app
COPY src/nifi_setup/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/nifi_setup/setup_flow.py .
COPY src/nifi_setup/jolt_spec.json .

# Elasticsearch mapping and Kibana saved objects are passed in at build time
# via build context from project root
COPY elasticsearch/events_mapping.json ./events_mapping.json
COPY kibana/saved_objects.ndjson ./saved_objects.ndjson

CMD ["python", "setup_flow.py"]
