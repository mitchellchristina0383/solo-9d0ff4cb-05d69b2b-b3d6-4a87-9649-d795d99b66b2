FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    PORT=8080 \
    HOST=0.0.0.0

RUN useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin logarchive \
    && mkdir -p /data \
    && chown -R logarchive:logarchive /data

# The named volume is created with root ownership on first use; the image's
# data dir is chowned above so a bind mount of an existing directory must be
# writable by uid 10001 itself.

WORKDIR /app
COPY logarchive/ /app/logarchive/

USER logarchive
VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()"]

CMD ["python3", "-m", "logarchive.app"]
