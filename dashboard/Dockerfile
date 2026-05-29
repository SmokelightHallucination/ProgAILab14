# Streamlit real-time dashboard (advanced task #6).
FROM python:3.12-slim
WORKDIR /app
COPY dashboard/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# The dashboard reuses aqanalyzer.stream from the analyzer package.
COPY analyzer/ /analyzer/
COPY dashboard/ ./
ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1 PYTHONPATH=/analyzer
EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", \
    "--server.address=0.0.0.0", "--server.port=8501", \
    "--server.headless=true"]
