# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (for NATS and other libs)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    netcat-openbsd \
    nats-server \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create a startup script to run NATS and all Python scripts
RUN echo '#!/bin/bash\n\
nats-server -DV &\n\
echo "Waiting for NATS to start..."\n\
until nc -z localhost 4222; do\n\
  sleep 1\n\
done\n\
echo "NATS is ready. Starting Python scripts..."\n\
python run_publisher.py &\n\
python run_detector.py &\n\
python run_report.py &\n\
python run_feedback.py &\n\
wait' > /app/start.sh && chmod +x /app/start.sh

# Expose NATS port (internal only, no host binding needed)
EXPOSE 4222

# Run the startup script
CMD ["/app/start.sh"]