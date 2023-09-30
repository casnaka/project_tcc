from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from delta.tables import *

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
        "--database", type=str, help="Database para processar", required=True,
    )
    parser.add_argument("--user", type=str, required=True, help="usuario para database")
    parser.add_argument(
        "--password", type=str, help="senha para conectar no database", required=True,
    )
    parser.add_argument(
        "--query", type=str, help="query para extrair os dados", required=True,
    )
    parser.add_argument("--table_name", type=str, help="nome da tabela", required=True)
    parser.add_argument("--ds", type=str, help="data de processamento", required=True)
    return parser.parse_args()


def process(spark, database: str, user: str, password: str, query: str):

    df = (
        spark.read.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", f"jdbc:mysql://localhost:3306/{database}")
        .option("database", database)
        .option("query", query)
        .option("user", user)
        .option("password", password)
        .load()
    )

    return df


def save(df, table_name: str):
    df.coalesce(1).write.mode("append").format("parquet").partitionBy(
        ["year", "month", "date"]
    ).option("parquet.compress", "snappy").save(f"/opt/process/raw/{table_name}")


arg = add_arguments(argparse.ArgumentParser())
query_final = arg.query.replace("<filter>", arg.ds)
spark = spark_session()
df = process(spark, arg.database, arg.user, arg.password, query_final)

save(df, arg.save_path, arg.table_name)
