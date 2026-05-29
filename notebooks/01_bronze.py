# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze
# MAGIC
# MAGIC Ingestão raw → bronze. Schema permissivo (string onde há ruído), metadados de carga.
# MAGIC
# MAGIC Uma tabela bronze por fonte. Sem transformação de domínio.

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

CATALOG = "workspace"
SCHEMA  = "desafio_bronze"
RAW     = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

def add_meta(df, source_file):
    return (df
        .withColumn("nom_arquivo_origem", F.lit(source_file))
        .withColumn("dt_ingestao", F.current_timestamp()))

def write_bronze(df, name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"{CATALOG}.{SCHEMA}.{name}: {df.count()} linhas")

# COMMAND ----------

# MAGIC %md ## erp_pedidos_cabecalho_2025.csv
# MAGIC Separador `;`, contém JSON aninhado em `payment_details` com aspas escapadas.

# COMMAND ----------

df = (spark.read
      .option("header", True)
      .option("sep", ";")
      .option("quote", '"')
      .option("escape", '"')
      .option("multiLine", True)
      .csv(f"{RAW}/erp_pedidos_cabecalho_2025.csv"))

write_bronze(add_meta(df, "erp_pedidos_cabecalho_2025.csv"), "pedidos_cabecalho")

# COMMAND ----------

# MAGIC %md ## erp_pedidos_itens_2025.csv
# MAGIC Separador `,`. 
# MAGIC
# MAGIC O campo `unit_price` pode ter vírgula decimal entre aspas.

# COMMAND ----------

df = (spark.read
      .option("header", True)
      .option("sep", ",")
      .option("quote", '"')
      .csv(f"{RAW}/erp_pedidos_itens_2025.csv"))

write_bronze(add_meta(df, "erp_pedidos_itens_2025.csv"), "pedidos_itens")

# COMMAND ----------

# MAGIC %md ## vendedores.csv
# MAGIC Separador `;`.

# COMMAND ----------

df = (spark.read
      .option("header", True)
      .option("sep", ";")
      .csv(f"{RAW}/vendedores.csv"))

write_bronze(add_meta(df, "vendedores.csv"), "vendedores")

# COMMAND ----------

# MAGIC %md ## legado_regioes_pipe.txt
# MAGIC Separador `|`.

# COMMAND ----------

df = (spark.read
      .option("header", True)
      .option("sep", "|")
      .csv(f"{RAW}/legado_regioes_pipe.txt"))

write_bronze(add_meta(df, "legado_regioes_pipe.txt"), "regioes")

# COMMAND ----------

# MAGIC %md ## comercial_canais.xlsx
# MAGIC XLSX. Lemos com pandas (volume pequeno: 8 linhas) e convertemos para Spark.
# MAGIC Atenção: `pd.read_excel(dtype=str)` converte NaN para a string literal "nan";
# MAGIC normalizamos para None antes de criar o DataFrame.

# COMMAND ----------

import pandas as pd
import numpy as np

def pandas_to_spark(pdf):
    """Substitui NaN / 'nan' string por None antes de createDataFrame."""
    pdf = pdf.replace({np.nan: None, "nan": None, "NaN": None, "NAN": None})
    return spark.createDataFrame(pdf)

pdf = pd.read_excel(f"{RAW}/comercial_canais.xlsx", sheet_name="canais", dtype=str)
write_bronze(add_meta(pandas_to_spark(pdf), "comercial_canais.xlsx"), "canais")

# COMMAND ----------

# MAGIC %md ## crm_clientes_export.xlsx

# COMMAND ----------

pdf = pd.read_excel(f"{RAW}/crm_clientes_export.xlsx", sheet_name=0, dtype=str)
write_bronze(add_meta(pandas_to_spark(pdf), "crm_clientes_export.xlsx"), "clientes")

# COMMAND ----------

# MAGIC %md ## cadastro_produtos_api_dump.json
# MAGIC JSON aninhado (array). Lemos com `multiLine=true` e preservamos estrutura.

# COMMAND ----------

df = (spark.read
      .option("multiLine", True)
      .json(f"{RAW}/cadastro_produtos_api_dump.json"))

write_bronze(add_meta(df, "cadastro_produtos_api_dump.json"), "produtos")

# COMMAND ----------

# MAGIC %md ## logistica_entregas.json
# MAGIC JSON array aninhado.

# COMMAND ----------

df = (spark.read
      .option("multiLine", True)
      .json(f"{RAW}/logistica_entregas.json"))

write_bronze(add_meta(df, "logistica_entregas.json"), "entregas")

# COMMAND ----------

# MAGIC %md ## atendimento_ocorrencias.ndjson
# MAGIC NDJSON (1 objeto por linha) — `multiLine=false`.

# COMMAND ----------

df = (spark.read
      .option("multiLine", False)
      .json(f"{RAW}/atendimento_ocorrencias.ndjson"))

write_bronze(add_meta(df, "atendimento_ocorrencias.ndjson"), "atendimento")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
