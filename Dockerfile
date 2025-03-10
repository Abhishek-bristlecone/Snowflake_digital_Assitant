# Use an official Python runtime as the base image
FROM python:3.10.11

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements and application files
COPY requirements.txt .
COPY utils.py .
COPY instructions.txt .
COPY app.py .
COPY .env .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the application port
EXPOSE 5000

# Define the command to run the Flask app
CMD ["python", "app.py"]
