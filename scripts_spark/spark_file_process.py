from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession

import argparse


def spark_session():
    return (
        SparkSession.builder.appName("process_file")
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
        "--type",
        type=str,
        help="tipo do arquivo a ser processado",
        required=True,
        choices=["json", "csv"],
    )
    parser.add_argument(
        "--raw_file_path", type=str, required=True, help="caminho para os arquivos"
    )
    parser.add_argument("--table_name", type=str, help="nome da tabela", required=True)
    parser.add_argument("--ds", type=str, help="data de processamento", required=True)
    return parser.parse_args()


def process(spark, type, raw_file_path, ds):
    from datetime import datetime
    
    df = (
        spark.read.format(type)
        .option("recursiveFileLookup", "true")
        .load(raw_file_path)
    )
    

    # criando uma coluna date do momento que foi processado
    df = df.withColumn("date", F.to_date(F.lit(ds), "yyyy-MM-dd"))

    df = df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

    
    return df


def save(df, table_name):
    df = df.coalesce(1)
    df.write.mode("append").format("parquet").partitionBy(
        ["year", "month", "date"]
    ).option("parquet.compress", "snappy").save(f"/opt/process/raw/{table_name}")


arg = add_arguments(argparse.ArgumentParser())
spark = spark_session()
# sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
df = process(spark,arg.type, arg.raw_file_path, arg.ds)
df.printSchema()
save(df, arg.table_name)
