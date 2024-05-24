FROM quay.io/astronomer/astro-runtime:11.3.0
USER root

# Update package lists and install PostgreSQL and Python development files
RUN apt-get update && apt-get install -y libpq-dev gcc

# Install dbt into a virtual environment
RUN python -m venv venv \
    && source venv/bin/activate \
    && pip install --no-cache-dir soda-core-snowflake \
    pip install --no-cache-dir soda-core-scientific \
    && pip install --no-cache-dir dbt-snowflake \
    && pip install --no-cache-dir dbt-postgres \
    && deactivate