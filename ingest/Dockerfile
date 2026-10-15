FROM python:3.12-slim

# ffmpeg is required by faster-whisper to decode arbitrary audio containers
# (opus/m4a/3gp/whatever Automate hands us).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Cache Whisper model weights on the mounted volume so rebuilds don't re-download.
ENV HF_HOME=/models \
    PYTHONUNBUFFERED=1 \
    LD_LIBRARY_PATH=/usr/local/lib/python3.12/site-packages/nvidia/cublas/lib:/usr/local/lib/python3.12/site-packages/nvidia/cudnn/lib

EXPOSE 8000

# Single worker on purpose: the Whisper model is loaded once into memory and
# transcription is serialized through one background worker task.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
