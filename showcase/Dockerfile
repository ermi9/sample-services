FROM python:3.12-slim
WORKDIR /app
COPY showcase/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY showcase/showcase.py .
COPY flows flows
ENTRYPOINT ["python", "-u", "showcase.py"]
