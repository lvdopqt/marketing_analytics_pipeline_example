FROM python:3.10-slim

WORKDIR /app

# Copy only requirements first for better caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of your code
COPY . .

CMD ["python", "scripts/monitor_data.py"]
