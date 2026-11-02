"""
Módulo de Transformações e Camada Silver
Projeto: Retail Analytics Lakehouse (PySpark & Delta Lake)
Autor: Eduardo de Castro Bello

Responsável por aplicar schemas estritos, limpeza de dados (preços, datas)
e validações de qualidade (Quality Gates) sobre os dados brutos.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)


def get_sales_schema() -> StructType:
    """Define o schema estrito para a tabela de vendas (sales)."""
    return StructType([
        StructField("sale_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("sale_date", StringType(), True),  # Convertido posteriormente para Data
    ])


def get_users_schema() -> StructType:
    """Define o schema estrito para a tabela de usuários (users)."""
    return StructType([
        StructField("client_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])


def get_products_schema() -> StructType:
    """Define o schema estrito para a tabela de produtos (products)."""
    return StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),  # Virá em formato string suja para ser tratado
    ])


def transform_sales(df: DataFrame) -> DataFrame:
    """Aplica limpeza e tipagem na camada Silver de vendas."""
    return (
        df
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("sale_date", F.to_date(F.col("sale_date"), "yyyy-MM-dd"))
        .dropDuplicates(["sale_id"])
    )


def transform_users(df: DataFrame) -> DataFrame:
    """Aplica limpeza e tipagem na camada Silver de usuários."""
    return (
        df
        .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
        .dropDuplicates(["client_id"])
    )


def transform_products(df: DataFrame) -> DataFrame:
    """
    Aplica limpeza na camada Silver de produtos.
    Trata a coluna de preço removendo caracteres indesejados (ex: símbolos de moeda)
    e convertendo para Double.
    """
    cleaned_price = (
        F.regexp_replace(F.col("price"), "[R\\$\\s]", "")
        .reg_replace(",", ".")
        .cast(DoubleType())
    )
    
    return (
        df
        .withColumn("price", cleaned_price)
        .dropDuplicates(["product_id"])
    )


def run_silver_quality_gates(df_sales: DataFrame, df_products: DataFrame) -> None:
    """
    Executa travas de qualidade (Quality Gates) antes de persistir a camada Silver.
    Valida nulos em chaves e integridade referencial (produtos órfãos).
    """
    null_keys = df_sales.filter(
        F.col("sale_id").isNull() | 
        F.col("client_id").isNull() | 
        F.col("product_id").isNull()
    ).count()
    
    assert null_keys == 0, "[DATA QUALITY ERROR] Encontradas chaves nulas na tabela de vendas!"

    # Checagem de produtos órfãos (vendas apontando para produtos inexistentes)
    orphan_count = df_sales.join(df_products, "product_id", "left_anti").count()
    assert orphan_count == 0, f"[DATA QUALITY ERROR] Existem {orphan_count} vendas com produtos órfãos!"

    print("[SUCCESS] Todos os Quality Gates da Camada Silver foram aprovados com sucesso!")