FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir .
ENV TME_DB=/data/tme.db TME_HOST=0.0.0.0 TME_PORT=8080
VOLUME ["/data"]
EXPOSE 8080
CMD ["tme", "serve"]

