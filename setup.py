#!/usr/bin/env python3
"""
Script de instalação para o projeto ETL de despesas de fundos.
"""
from setuptools import setup, find_packages

setup(
    name="fund_expenses_etl",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "openpyxl>=3.0.7",
        "python-dateutil>=2.8.2",
        "pyyaml>=6.0",
        "click>=8.0.0",
        "tqdm>=4.62.0",
        "loguru>=0.6.0",
        "sqlalchemy>=1.4.0",
        "pyarrow>=6.0.0",
        "mysql-connector-python>=8.0.0",
        "python-dotenv>=0.19.0",
    ],
    entry_points={
        "console_scripts": [
            "fund-etl=src.main:cli",
        ],
    },
)