# Use Python slim image for smaller footprint
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install required Python packages
RUN pip install --no-cache-dir dbt-graph-loader redis

# Default command to run the graph loader
CMD ["dbt-graph-loader", "falkordb", "--host", "falkordb", "--port", "6379", "--manifest", "/dbt/target/manifest.json", "--catalog", "/dbt/target/catalog.json"]
