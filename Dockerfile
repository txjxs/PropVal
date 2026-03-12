# 1. Start with a lightweight Python Linux image
FROM python:3.11-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy your requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy your source code, config, and credential key
COPY src/ ./src/
COPY config/ ./config/
COPY service-account.json .
# Note: We do NOT copy .env for security. We pass secrets at runtime.

# 5. Set ENTRYPOINT to allow mode parameter
# Usage: docker run <image> sold       -> runs with 'sold' mode
#        docker run <image> forSale    -> runs with 'forSale' mode
#        docker run <image>            -> runs with default 'forSale' mode
ENTRYPOINT ["python", "src/ingest.py"]
CMD ["forSale"]