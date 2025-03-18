# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Set the environment variable for Flask
ENV FLASK_APP=main.py

# Expose the port the app runs on
EXPOSE 8080

# Run the application
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]
