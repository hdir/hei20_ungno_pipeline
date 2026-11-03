# syntax=docker/dockerfile:1
FROM python:3.11-slim

# --- pyodbc + Microsoft ODBC Driver 18 ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg ca-certificates unixodbc unixodbc-dev gcc g++ \
  && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/microsoft-prod.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# CPU-torch først (mye mindre image enn standard CUDA-hjulet), deretter resten
COPY requirements.txt .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
  && pip install --no-cache-dir -r requirements.txt

# Last ned embedding-modellen ved bygg, så jobben ikke er avhengig av Hugging Face ved kjøring
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"
ENV HF_HUB_OFFLINE=1

COPY run_pipeline.py ./
COPY pipeline ./pipeline

ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "run_pipeline.py"]
