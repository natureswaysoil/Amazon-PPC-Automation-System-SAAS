# Use official Python runtime
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
# gcc is needed for some Python packages that compile C extensions
RUN apt-get update \
  && apt-get install -y gcc \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- MAIN CODE COPY ---
# Copy ALL application code from your local project root into the container's /app directory.
# This ensures the 'automation' folder and its contents are present.
COPY . .

# Set Python path to include the /app directory so Python can find your modules.
ENV PYTHONPATH=/app

# Ensure Python output is not buffered (critical for Cloud Run logging)
ENV PYTHONUNBUFFERED=1

# Tell the container what command to run when it starts.
# This executes the bid_optimizer module inside the automation package.
CMD ["python", "main.py"]
