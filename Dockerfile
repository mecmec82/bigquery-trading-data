# Use a lightweight Python base image
# We use a specific version for reproducibility
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file first to leverage Docker's layer caching
# This helps speed up rebuilds if only the code changes, not dependencies
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# IMPORTANT: In Cloud Run, you should use IAM service accounts
# and not embed the key file directly in the container.
# This line is included for completeness if you MUST use a key file
# (e.g., for local testing), but it's NOT RECOMMENDED for Cloud Run deployment.
# DELETE OR COMMENT OUT THIS LINE FOR PRODUCTION CLOUD RUN DEPLOYMENT
# ENV GOOGLE_APPLICATION_CREDENTIALS="/app/ml8849-902e0838d83c.json" 

# Command to run your script when the container starts
# This assumes your script is named download_upload_stock_data.py
CMD ["python", "upload-data.py"] 
