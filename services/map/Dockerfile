# Install base image
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Ensure Python output is viewable in real time and not buffered
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Sync dependencies
RUN uv sync --frozen --no-install-project

# Copy remainder of code
COPY ./src ./src

# Install package
RUN uv pip install --no-deps .

# Set up package entrypoint script
ENTRYPOINT ["uv", "run", "python", "-m", "map_clean.main"]