FROM quay.io/astronomer/astro-runtime:10.1.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-bigquery==1.5.3 && deactivate