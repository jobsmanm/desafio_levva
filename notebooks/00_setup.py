# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC
# MAGIC Cria catalog/schemas/volume usados pela solução.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA_BRONZE = "desafio_bronze"
SCHEMA_SILVER = "desafio_silver"
SCHEMA_GOLD   = "desafio_gold"
VOLUME_RAW    = "raw"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{VOLUME_RAW}")

print(f"Volume raw em: /Volumes/{CATALOG}/{SCHEMA_BRONZE}/{VOLUME_RAW}/")
print("Suba os 9 arquivos de sources/ para esse caminho pela UI (Catalog → Volume → Upload).")

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/{VOLUME_RAW}/"))
