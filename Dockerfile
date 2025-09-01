FROM astrocrpublic.azurecr.io/runtime:3.0-5

# Copy requirements and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
