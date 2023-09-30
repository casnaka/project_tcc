from pyspark.sql.functions import current_date, year, month
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import ast

import argparse


def spark_session():
    return (
        SparkSession.builder.appName("process_rdbms")
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .enableHiveSupport()
        .getOrCreate()
    )


def add_arguments(parser):
    parser = argparse.ArgumentParser(
        prog="Spark Process", description="Processar arquivos "
    )
    parser.add_argument(
        "--parquet_path",
        type=str,
        help="local onde os arquivos serao lidos",
        required=True,
    )
    parser.add_argument(
        "--query", type=str, help="refinar os dados", required=True,
    )
    parser.add_argument("--table_name", type=str, help="nome da tabela", required=True)
    parser.add_argument(
        "--partition", type=ast.literal_eval, help="dado sera particionado", required=True,
    )
    return parser.parse_args()


def process(spark, parquet_path: str, query: str):

    df = spark.read.parquet(parquet_path)
    df.cache()

    # criando uma coluna date do momento que foi processado
    df.createOrReplaceTempView("temp_view")
    df = spark.sql(query)
    spark.catalog.dropTempView("temp_view")
    df.unpersist()
    return df


def save(df, table_name: str, partition: list = ["year", "month", "date"]):
    df.coalesce(1).write.mode("append").format("parquet").partitionBy(list(partition)).option("parquet.compress", "snappy").save(f"/opt/process/refined/{table_name}")


arg = add_arguments(argparse.ArgumentParser())
spark = spark_session()
df = process(spark, arg.database, arg.user, arg.password, arg.query)

save(df, arg.save_path, arg.table_name)
