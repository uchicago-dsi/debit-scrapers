# Start with Python base image
FROM python:3.8-slim

# Create working directory and add to path variable
ARG PROJECTDIR=/usr/src
WORKDIR $PROJECTDIR
ENV PYTHONPATH "${PYTHONPATH}:${PROJECTDIR}"

# Install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip==21.3.1
RUN pip install --no-cache-dir -r requirements.txt

# Copy remainder of code
ARG ENV
COPY scrapers/ scrapers/
COPY "config.${ENV}.yaml" "config.${ENV}.yaml"

# Start server
EXPOSE 5000
EXPOSE 5050
CMD ["python3", "./scrapers/entrypoints/queue_workflows.py"]
