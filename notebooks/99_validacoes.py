# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 99 - Validações de Qualidade
# MAGIC
# MAGIC Checagens automáticas sobre as camadas silver e gold.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER  = f"{CATALOG}.desafio_silver"
GOLD    = f"{CATALOG}.desafio_gold"

# COMMAND ----------

# MAGIC %md ## Contagens por tabela

# COMMAND ----------

for schema in (SILVER, GOLD):
    print(f"=== {schema} ===")
    tbls = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {schema}").collect()]
    for t in tbls:
        c = spark.table(f"{schema}.{t}").count()
        print(f"  {t:30s} {c:>8} linhas")

# COMMAND ----------

# MAGIC %md ## Qualidade — flags técnicas (consulta na silver)

# COMMAND ----------

print("Itens com quantity inválida (≤0):",
      spark.table(f"{SILVER}.pedidos_itens").filter("quantity_invalida").count())
print("Itens com total divergente:",
      spark.table(f"{SILVER}.pedidos_itens").filter("total_divergente").count())
print("Entregas com pedido órfão:",
      spark.table(f"{SILVER}.entregas").filter("pedido_orfao").count())
print("Entregas com data envio inválida:",
      spark.table(f"{SILVER}.entregas").filter("data_envio_invalida").count())
print("Entregas com data entrega inválida:",
      spark.table(f"{SILVER}.entregas").filter("data_entrega_invalida").count())
print("Clientes com email inválido:",
      spark.table(f"{SILVER}.clientes").filter("not email_valido").count())

# COMMAND ----------

# MAGIC %md ## SCD2 — versões por entidade

# COMMAND ----------

print("dim_cliente total:", spark.table(f"{GOLD}.dim_cliente").count())
print("dim_cliente flg_atual=true:", spark.table(f"{GOLD}.dim_cliente").filter("flg_atual=true").count())
print("dim_cliente flg_atual=false (versões expiradas):", spark.table(f"{GOLD}.dim_cliente").filter("flg_atual=false").count())

print("\ndim_produto total:", spark.table(f"{GOLD}.dim_produto").count())
print("dim_produto flg_atual=true:", spark.table(f"{GOLD}.dim_produto").filter("flg_atual=true").count())
print("dim_produto flg_atual=false:", spark.table(f"{GOLD}.dim_produto").filter("flg_atual=false").count())

# COMMAND ----------

# MAGIC %md ## Métricas de negócio

# COMMAND ----------

# MAGIC %md ### Receita por status

# COMMAND ----------

display(spark.sql(f"""
    SELECT sts_pedido,
           COUNT(*)                       AS pedidos,
           ROUND(SUM(val_liquido), 2)     AS val_liquido_total,
           ROUND(AVG(val_liquido), 2)     AS ticket_medio
    FROM {GOLD}.fato_pedido_cabecalho
    GROUP BY sts_pedido
    ORDER BY pedidos DESC
"""))

# COMMAND ----------

# MAGIC %md ### Taxa de cancelamento por canal

# COMMAND ----------

display(spark.sql(f"""
    SELECT c.nom_canal,
           COUNT(*)                                                                  AS pedidos,
           SUM(CASE WHEN f.flg_cancelado THEN 1 ELSE 0 END)                          AS cancelados,
           ROUND(100.0 * SUM(CASE WHEN f.flg_cancelado THEN 1 ELSE 0 END)/COUNT(*), 2) AS pct_cancel
    FROM {GOLD}.fato_pedido_cabecalho f
    LEFT JOIN {GOLD}.dim_canal c USING(cod_canal)
    GROUP BY c.nom_canal
    ORDER BY pedidos DESC
"""))

# COMMAND ----------

# MAGIC %md ### Taxa de atraso por região

# COMMAND ----------

display(spark.sql(f"""
    SELECT r.nom_regional,
           COUNT(*)                                                                       AS entregas,
           SUM(CASE WHEN e.flg_com_atraso THEN 1 ELSE 0 END)                              AS com_atraso,
           ROUND(100.0 * SUM(CASE WHEN e.flg_com_atraso THEN 1 ELSE 0 END)/COUNT(*), 2)   AS pct_atraso
    FROM {GOLD}.fato_entrega e
    JOIN {GOLD}.fato_pedido_cabecalho p USING(cod_pedido)
    LEFT JOIN {GOLD}.dim_regiao r ON r.cod_regional = p.cod_regional
    WHERE e.dt_entrega IS NOT NULL AND p.dt_promessa IS NOT NULL
    GROUP BY r.nom_regional
    ORDER BY entregas DESC
"""))

# COMMAND ----------

# MAGIC %md ### Receita por categoria de produto (via SCD2 — produto vigente no momento da venda)

# COMMAND ----------

display(spark.sql(f"""
    SELECT p.des_categoria,
           COUNT(DISTINCT i.cod_pedido)       AS pedidos,
           ROUND(SUM(i.val_receita_item), 2)  AS val_receita
    FROM {GOLD}.fato_pedido_item i
    LEFT JOIN {GOLD}.dim_produto p ON p.sk_produto = i.sk_produto
    GROUP BY p.des_categoria
    ORDER BY val_receita DESC NULLS LAST
"""))
