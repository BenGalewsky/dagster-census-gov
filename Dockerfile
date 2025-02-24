FROM python:3.12
WORKDIR /project
COPY pyproject.toml /project/

RUN pip install .
COPY dagster_census_gov/ /project/dagster_census_gov/
RUN pip install -e .
