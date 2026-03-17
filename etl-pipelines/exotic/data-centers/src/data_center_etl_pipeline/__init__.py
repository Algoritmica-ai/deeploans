"""Data-center ETL pipeline package with bronze/silver/gold stages."""

from .pipeline import run_etl

__all__ = ["run_etl"]
