# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 99 - Reset
# MAGIC
# MAGIC Dropa objetos criados pela solução para permitir re-execução limpa.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA_BRONZE = "desafio_bronze"
SCHEMA_SILVER = "desafio_silver"
SCHEMA_GOLD   = "desafio_gold"
VOLUME_RAW    = "raw"

# COMMAND ----------

# MAGIC %md ## Opção 1: Reset parcial (mantém volume + arquivos)

# COMMAND ----------

# Drop tabelas das 3 camadas, depois os schemas silver e gold (sem cascade no bronze pra preservar volume)
for schema in (SCHEMA_SILVER, SCHEMA_GOLD):
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
    print(f"DROP SCHEMA {CATALOG}.{schema} CASCADE")

# Bronze: dropa só as tabelas, preserva o schema e o volume
tbls = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA_BRONZE}").collect()
for r in tbls:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_BRONZE}.{r['tableName']}")
    print(f"DROP TABLE {CATALOG}.{SCHEMA_BRONZE}.{r['tableName']}")

print("\nReset parcial concluído. Volume e arquivos preservados.")
