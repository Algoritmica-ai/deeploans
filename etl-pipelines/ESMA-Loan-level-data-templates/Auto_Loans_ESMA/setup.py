from setuptools import setup, find_packages

setup(
    name="aut_etl_pipeline",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "google-cloud-storage",
        "pandas",
        "cerberus",
        "pyspark",
    ],
) 