from setuptools import setup, find_packages

setup(
    name="etl-pipeline-project",
    version="1.0.0",
    author="Vineeth Reddy Aredla",
    author_email="aredlavineethreddy2001@gmail.com",
    description="Production-grade cloud ETL pipeline with PySpark, Airflow, AWS, and Snowflake",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/vineethreddy-a/cloud-etl-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "pandas>=2.0",
        "boto3>=1.34",
        "pyspark>=3.5",
        "PyYAML>=6.0",
        "python-dotenv>=1.0",
        "requests>=2.31",
    ],
    extras_require={
        "dev": ["pytest", "pytest-cov", "flake8", "black"],
        "snowflake": ["snowflake-connector-python", "snowflake-sqlalchemy"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
