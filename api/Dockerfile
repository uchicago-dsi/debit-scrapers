# Retrieve base Python image
FROM python:3.9

# Create new working directory within container
WORKDIR /src/api

# Ensure Python output is viewable in real time and not buffered
ENV PYTHONUNBUFFERED=1

# Copy requirements.txt file and install packages
COPY requirements.txt .
RUN pip install --upgrade pip==22.0.4
RUN pip install --no-cache-dir -r requirements.txt

# Create new directory for cache if not already created
RUN mkdir -p ./tmp/cache; exit 0

# Copy project code
COPY . .

# Expose API port
EXPOSE 8080

# Run bash script on container start
CMD ["bash", "setup.sh", "--migrate", "--load"]