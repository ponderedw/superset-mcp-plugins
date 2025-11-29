# Use Python slim image for smaller footprint
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install required Python packages
RUN pip install --no-cache-dir requests sqlalchemy psycopg2-binary

# Copy the imports folder (including import_project.py and all .zip files)
COPY ./ /app/

# Make the script executable
RUN chmod +x /app/import_project.py

# Default command to run the import script
CMD ["python", "/app/import_project.py"]