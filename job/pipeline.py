import argparse
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from job.bronze import run_bronze
from job.silver import run_silver
from job.gold import run_gold

# Ensure Spark uses the current Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Set HADOOP_HOME and java.library.path for Windows (needed for native Hadoop IO)
if os.name == "nt":
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = "C:/hadoop"
    hadoop_bin_path = os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["PATH"] = hadoop_bin_path + ";" + os.environ.get("PATH", "")
    # Must set before JVM starts; spark.driver.extraJavaOptions is ignored in local mode
    os.environ["_JAVA_OPTIONS"] = (
        os.environ.get("_JAVA_OPTIONS", "")
        + f" -Djava.library.path={hadoop_bin_path}"
    )


# ----------------------------
# Config
# ----------------------------
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ----------------------------
# Spark
# ----------------------------
def get_spark(app_name: str = "de-pipeline") -> SparkSession:
    hadoop_bin = os.path.join(os.environ.get("HADOOP_HOME", "C:/hadoop"), "bin")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if os.name == "nt":
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Djava.library.path={hadoop_bin}",
        )
    return builder.getOrCreate()


# ----------------------------
# Main
# ----------------------------
def main(config_path: str):
    config = load_config(config_path)
    spark = get_spark()

    # Resolve paths relative to project root (parent of config directory)
    project_root = Path(config_path).resolve().parent.parent
    raw_path = str(project_root / config["paths"]["raw_events"])
    users_path = str(project_root / config["paths"]["users"])
    output_base = str(project_root / config["paths"]["output"])

    print("=== Running Bronze Layer ===")
    run_bronze(spark, raw_path, output_base)

    print("=== Running Silver Layer ===")
    run_silver(spark, output_base, users_path)

    print("=== Running Gold Layer ===")
    run_gold(spark, output_base)

    print("=== Pipeline Complete ===")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)
