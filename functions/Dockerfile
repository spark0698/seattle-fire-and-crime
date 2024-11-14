FROM python:3.10.6-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

ENV PORT=8080

CMD ["functions-framework", "--target", "extract_data", "--source", "extract-seattle-data.py"]