From python:3.7

Workdir /app

Copy src src
Copy requirements.txt .

Run pip install -r requirements.txt

ENTRYPOINT ["python", "src/main.py"]