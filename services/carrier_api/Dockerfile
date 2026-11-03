# The mock carrier API as a container, for Azure Container Apps (infra/carrier_api.tf).
# Built by scripts/up.ps1 right after it generates the day's data, from the repo root:
#   docker build -f services/carrier_api/Dockerfile -t <registry>/carrier-api:<tag> .
# The image carries that day's shipments (data/shipments/shipments.jsonl): they belong to the same
# orders as the rest of the session's data, so the API serves exactly what the file feed would have.
# The API key is not in the image: the Container App passes it as CARRIER_API_KEY from its secret.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# Same versions as requirements-dev.txt (tests/test_carrier_api_deploy.py keeps them in step)
RUN pip install --no-cache-dir fastapi==0.136.1 uvicorn==0.46.0

COPY services/__init__.py services/__init__.py
COPY services/carrier_api/__init__.py services/carrier_api/app.py services/carrier_api/
COPY data/shipments/shipments.jsonl data/shipments/shipments.jsonl

# Port 80: the same port as the placeholder image the Container App is first created with, so the
# app is healthy before and after the first real image. Runs as root inside the container for that
# (a port below 1024); the container holds no secret but the API key and no write access anywhere.
EXPOSE 80
CMD ["uvicorn", "services.carrier_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "80"]
