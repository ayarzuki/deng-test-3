import os
import sys
import pytest
from pyspark.sql import SparkSession

# Ensure Spark uses the current Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Set Hadoop env for Windows before JVM starts
if os.name == "nt":
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = "C:/hadoop"
    hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["PATH"] = hadoop_bin + ";" + os.environ.get("PATH", "")
    os.environ["_JAVA_OPTIONS"] = (
        os.environ.get("_JAVA_OPTIONS", "")
        + f" -Djava.library.path={hadoop_bin}"
    )


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-pipeline")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    yield session
    session.stop()
