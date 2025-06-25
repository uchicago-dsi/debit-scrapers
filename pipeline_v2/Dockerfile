# Retrieve base Python image
FROM selenium/standalone-chrome

# Switch to root user for Linux installations
USER root

# Create new working directory within container
WORKDIR /src

# Ensure Python output is viewable in real time and not buffered
ENV PYTHONUNBUFFERED=1

# Copy requirements file and install Python packages
COPY requirements.txt .
RUN apt-get update -y
RUN apt-get install -y python3 python3-pip
RUN pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Run Playwright install
RUN python3 -m playwright install

# Copy remainder of code
COPY . .

# Expose API port
EXPOSE 8080

# Run bash script on container start
CMD ["python3", "./manage.py", "runserver", "0.0.0.0:8080"]