FROM python:3.11-slim

RUN mkdir /app
WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3","/app/main.py"]
