FROM astrocrpublic.azurecr.io/runtime:3.0-5

# Install uv + hatchling, then install project deps from pyproject.toml
COPY pyproject.toml ./
RUN pip install --no-cache-dir uv \
    && uv pip install --system --no-cache .
