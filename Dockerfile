# Dockerfile
FROM python:3.9 

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all files from the current directory to the root of the container
COPY . /

# Specify the command to run within the container
CMD [ "python", "weekly_updates.py" ]