FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (none for now)
# RUN apt-get update && apt-get install -y ...

# Install uv for high-speed dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
