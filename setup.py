from setuptools import find_packages, setup

setup(
    name="dagster_census_gov",
    packages=find_packages(exclude=["dagster_census_gov_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
