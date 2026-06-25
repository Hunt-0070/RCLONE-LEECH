FROM python:3.11-slim

# rclone + ffmpeg are required at runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        ca-certificates \
        curl \
        unzip \
        util-linux \
    && curl -fsSL https://rclone.org/install.sh | bash \
    && apt-get purge -y unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY config.env .
# Health check port (override with PORT/HEALTH_PORT)
EXPOSE 8080

CMD ["python", "bot.py"]
