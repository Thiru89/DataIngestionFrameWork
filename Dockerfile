FROM python:3.9.13
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1
CMD ["python", "dataIngestion.py"]