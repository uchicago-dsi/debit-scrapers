# Pull base image (NOTE: Higher versions of Chrome seem to break WebGL)
FROM selenium/standalone-chrome:104.0.5112.79

# Switch to root user for Linux installations
USER root

# Create new working directory within container for source code
WORKDIR /src

# Ensure Python output is viewable in real time and not buffered
ENV PYTHONUNBUFFERED=1

# Copy requirements file and install Python packages
COPY pipeline/requirements.txt .
RUN apt-get update -y
RUN apt-get install -y python python3-pip
RUN pip install --upgrade pip==22.0.4
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy remainder of code
COPY pipeline/ ./pipeline
COPY pyproject.toml config* ./

# Install pipeline package
RUN python3 -m pip install --editable .

# Start server
EXPOSE 5000
CMD ["python3", "./pipeline/queue_workflows.py"]
