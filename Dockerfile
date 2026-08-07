FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    signalrcore==0.9.5 \
    watchdog

COPY badcat.py .

RUN useradd -m -u 1000 badcat && chown -R badcat:badcat /app
USER badcat

CMD ["python", "badcat.py"]

