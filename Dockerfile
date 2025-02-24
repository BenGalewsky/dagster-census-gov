FROM python:3.12
COPY pyproject.toml /project/dagster_census_gov/

WORKDIR /project/dagster_census_gov/

RUN pip install .
COPY dagster_census_gov/ /project/dagster_census_gov/
RUN pip install -e .
