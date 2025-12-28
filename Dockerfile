# Use official Python runtime
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update \
  && apt-get install -y gcc \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- THE FIX IS HERE ---
# Copy ALL application code from your local project root into /app
# This includes the 'automation' folder AND any script outside it.
COPY . . 
# -----------------------

# Set Python path to include the /app directory
ENV PYTHONPATH=/app

# IMPORTANT: You need to tell the container what to run.
# If you are trying to run the module directly:
# CMD ["python", "-m", "automation.bid_optimizer"]

# OR if you have a main entrypoint script outside the automation folder:
# CMD ["python", "main.py"]
