# Use official Python runtime
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
# Combine commands for efficiency and smaller image size
# gcc is needed for some Python packages that compile C extensions
RUN apt-get update \
    && apt-get install -y gcc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
# This copies the entire 'automation' directory into '/app/automation/'
# Ensure your local directory structure matches this expectation (e.g., 'automation/bid_optimizer.py')
COPY automation/ ./automation/

# Set Python path to include the /app directory
# This allows Python to find modules within /app, including those in /app/automation
ENV PYTHONPATH=/app

# Default command (can be overridden when running the container)
# Use the full module path from PYTHONPATH
CMD ["python", "-m", "automation.bid_optimizer"]
