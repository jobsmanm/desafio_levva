# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 02 - Silver - Eventos
# MAGIC
# MAGIC bronze → silver para entidades transacionais: pedidos (cabecalho + itens), entregas, atendimento.
# MAGIC
# MAGIC Transformações principais:
# MAGIC - Parse multi-formato de datas
# MAGIC - Normalização de status (faturado, em_separacao, cancelado, entregue)
# MAGIC - `payment_details` (JSON aninhado em string) → struct
# MAGIC - `unit_price` com vírgula decimal → decimal
# MAGIC - Itens com `quantity <= 0` ou `total_item < 0` → flag de inválido
# MAGIC - Recalcula `total_item` consistente e detecta divergências
# MAGIC - Quarentena para entregas com `order_ref` órfão e datas impossíveis

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType

# Belt and suspenders: tenta desligar ANSI; mesmo se a config for ignorada (Free Edition serverless),
# os parses usam try_to_* abaixo, que retornam null em vez de exceção.
try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
except Exception as e:
    print(f"Config ignorada (esperado em serverless): {e}")

CATALOG = "workspace"
BRONZE  = f"{CATALOG}.desafio_bronze"
SILVER  = f"{CATALOG}.desafio_silver"

# ----- Helpers (idênticos ao 02_silver_dimensoes; reproduzidos para autonomia do notebook) -----

def upper_trim(col):
    return F.upper(F.trim(col))

def parse_date_multi(col):
    if isinstance(col, str):
        col = F.col(col)
    return F.coalesce(
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd")).cast("date"),
        F.try_to_timestamp(col, F.lit("yyyy/MM/dd")).cast("date"),
        F.try_to_timestamp(col, F.lit("dd/MM/yyyy")).cast("date"),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss")).cast("date"),
    )

def parse_ts_multi(col):
    if isinstance(col, str):
        col = F.col(col)
    return F.coalesce(
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(col, F.lit("yyyy/MM/dd")),
        F.try_to_timestamp(col, F.lit("dd/MM/yyyy HH:mm")),
        F.try_to_timestamp(col, F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
    )

UF_MAP = {
    "sp":"SP","sao paulo":"SP","são paulo":"SP",
    "rj":"RJ","rio de janeiro":"RJ",
    "mg":"MG","minas gerais":"MG",
    "sc":"SC","santa catarina":"SC","sta catarina":"SC","s. catarina":"SC",
    "pr":"PR","paraná":"PR","parana":"PR",
    "am":"AM","amazonas":"AM",
    "ba":"BA","bahia":"BA",
    "go":"GO","goiás":"GO","goias":"GO",

    # Demais estados / UFs
    "ac":"AC","acre":"AC",
    "al":"AL","alagoas":"AL",
    "ap":"AP","amapá":"AP","amapa":"AP",
    "ce":"CE","ceará":"CE","ceara":"CE",
    "df":"DF","distrito federal":"DF","brasília":"DF","brasilia":"DF",
    "es":"ES","espírito santo":"ES","espirito santo":"ES",
    "ma":"MA","maranhão":"MA","maranhao":"MA",
    "mt":"MT","mato grosso":"MT",
    "ms":"MS","mato grosso do sul":"MS",
    "pa":"PA","pará":"PA","para":"PA",
    "pb":"PB","paraíba":"PB","paraiba":"PB",
    "pe":"PE","pernambuco":"PE",
    "pi":"PI","piauí":"PI","piaui":"PI",
    "rn":"RN","rio grande do norte":"RN",
    "rs":"RS","rio grande do sul":"RS",
    "ro":"RO","rondônia":"RO","rondonia":"RO",
    "rr":"RR","roraima":"RR",
    "se":"SE","sergipe":"SE",
    "to":"TO","tocantins":"TO",
}

def norm_uf(col):
    c = F.lower(F.trim(col))
    expr = None
    for k,v in UF_MAP.items():
        cond = (c == F.lit(k))
        expr = F.when(cond, F.lit(v)) if expr is None else expr.when(cond, F.lit(v))
    return expr.otherwise(F.when(c.isNull(), F.lit(None)).otherwise(F.upper(F.trim(col))))

def norm_status_pedido(col):
    """Faturado/EM_SEPARACAO/cancelado/entregue + variações de casing/underscore"""
    c = F.lower(F.regexp_replace(F.trim(col), "_", " "))
    return (F.when(c == "faturado",      F.lit("faturado"))
             .when(c == "em separacao",  F.lit("em_separacao"))
             .when(c == "em separação",  F.lit("em_separacao"))
             .when(c == "cancelado",     F.lit("cancelado"))
             .when(c == "entregue",      F.lit("entregue"))
             .otherwise(F.lit(None)))

def norm_status_entrega(col):
    c = F.lower(F.trim(col))
    return (F.when(c == "delivered",  F.lit("entregue"))
             .when(c == "in_transit", F.lit("em_transito"))
             .when(c == "atrasado",   F.lit("atrasado"))
             .when(c == "cancelled",  F.lit("cancelado"))
             .otherwise(F.lit(None)))

def norm_modal(col):
    c = F.lower(F.trim(col))
    return (F.when(c.isin("rodoviário","rodoviario"), F.lit("rodoviario"))
             .when(c.isin("aéreo","aereo"),           F.lit("aereo"))
             .otherwise(F.lit(None)))

def norm_event_type(col):
    c = F.lower(F.trim(col))
    return (F.when(c == "refund",          F.lit("estorno"))
             .when(c == "troca",           F.lit("troca"))
             .when(c == "delay",           F.lit("atraso"))
             .when(c == "complaint",       F.lit("reclamacao"))
             .when(c == "cancel_request",  F.lit("cancelamento"))
             .otherwise(F.lit(None)))

def norm_severity(col):
    c = F.lower(F.trim(col))
    return F.when(c.isin("low","medium","high"), c).otherwise(F.lit(None))

def norm_status_ticket(col):
    c = F.lower(F.trim(col))
    return F.when(c.isin("open","closed"), c).otherwise(F.lit(None))

def write_silver(df, name):
    df = df.withColumn("dt_processamento_silver", F.current_timestamp())
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(f"{SILVER}.{name}"))
    print(f"{SILVER}.{name}: {df.count()} linhas")

# COMMAND ----------

# MAGIC %md ## pedidos
# MAGIC
# MAGIC - Parse `order_date` e `promised_date` (3 formatos)
# MAGIC - Normaliza status
# MAGIC - Cast de amounts (decimal 18,2)
# MAGIC - `payment_details` (JSON em string) → struct com `priority` e `source`

# COMMAND ----------

raw = spark.table(f"{BRONZE}.pedidos_cabecalho")

payment_schema = StructType([
    StructField("priority", StringType(), True),
    StructField("source",   StringType(), True),
])

clean = (raw
    .withColumn("order_id",       upper_trim("order_id"))
    .withColumn("customer_id",    upper_trim("customer_code"))
    .withColumn("seller_id",      upper_trim("seller_id"))
    .withColumn("order_date",     parse_date_multi("order_date"))
    .withColumn("promised_date",  parse_date_multi("promised_date"))
    .withColumn("status_pedido",  norm_status_pedido("status_order"))
    .withColumn("gross_amount",   F.col("gross_amount").cast("decimal(18,2)"))
    .withColumn("discount_amount",F.col("discount_amount").cast("decimal(18,2)"))
    .withColumn("net_amount",     F.col("net_amount").cast("decimal(18,2)"))
    .withColumn("payment",        F.from_json("payment_details", payment_schema))
    .withColumn("payment_priority", F.col("payment.priority"))
    .withColumn("payment_source",   F.col("payment.source"))
    .withColumn("last_update",    parse_ts_multi("last_update"))
    .select("order_id","customer_id","seller_id","order_date","promised_date",
            "status_pedido","gross_amount","discount_amount","net_amount",
            "payment_priority","payment_source","last_update"))

write_silver(clean, "pedidos")

# COMMAND ----------

# MAGIC %md ## pedidos_itens
# MAGIC
# MAGIC - `order_id` normalizado (UPPER) — casa com cabeçalho que tinha lowercase
# MAGIC - `unit_price` com vírgula decimal → ponto → decimal
# MAGIC - Flags: `quantity_invalida` (≤0), `total_divergente` (|qty*price - total| > 0.05)
# MAGIC - `item_status` normalizado

# COMMAND ----------

raw = spark.table(f"{BRONZE}.pedidos_itens")

clean = (raw
    .withColumn("order_id",   upper_trim("order_id"))
    .withColumn("item_seq",   F.col("item_seq").cast("int"))
    .withColumn("product_id", upper_trim("product_code"))
    .withColumn("quantity",   F.col("quantity").cast("decimal(18,3)"))
    .withColumn("unit_price",
                F.regexp_replace(F.col("unit_price"), ",", ".").cast("decimal(18,4)"))
    .withColumn("total_item", F.col("total_item").cast("decimal(18,2)"))
    .withColumn("item_status", F.lower(F.trim("item_status")))
    .withColumn("quantity_invalida", F.col("quantity") <= 0)
    .withColumn("total_calculado", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
    .withColumn("total_divergente",
                F.abs(F.col("total_calculado") - F.col("total_item")) > F.lit(0.05))
    .select("order_id","item_seq","product_id","quantity","unit_price","total_item",
            "total_calculado","item_status","quantity_invalida","total_divergente"))

write_silver(clean, "pedidos_itens")

# COMMAND ----------

# MAGIC %md ## entregas
# MAGIC
# MAGIC - Achata `carrier`, `timestamps`, `destination`
# MAGIC - Normaliza status, modal, UF
# MAGIC - `delivery_id` duplicado → dedup priorizando registro com `delivery_status` preenchido
# MAGIC - `order_ref` órfão → mantém no fato, sinaliza `pedido_orfao=true`
# MAGIC - Datas impossíveis (31/02) → null + flag

# COMMAND ----------

raw = spark.table(f"{BRONZE}.entregas")

clean = (raw
    .select(
        upper_trim("delivery_id").alias("delivery_id"),
        upper_trim("order_ref").alias("order_id"),
        F.col("carrier.name").alias("transportadora"),
        norm_modal(F.col("carrier.mode")).alias("modal"),
        norm_status_entrega("delivery_status").alias("status_entrega"),
        parse_ts_multi(F.col("timestamps.shipped_at")).alias("shipped_at"),
        parse_ts_multi(F.col("timestamps.delivered_at")).alias("delivered_at"),
        F.col("timestamps.shipped_at").alias("_shipped_at_raw"),
        F.col("timestamps.delivered_at").alias("_delivered_at_raw"),
        norm_uf(F.col("destination.state")).alias("uf_destino"),
        F.trim(F.col("destination.city")).alias("cidade_destino"),
        F.col("cost").cast("decimal(18,2)").alias("custo_frete"),
    ))

# Flags de data inválida (raw não-nulo mas parse falhou)
clean = (clean
    .withColumn("data_envio_invalida",
                F.col("_shipped_at_raw").isNotNull() & F.col("shipped_at").isNull())
    .withColumn("data_entrega_invalida",
                F.col("_delivered_at_raw").isNotNull() & F.col("delivered_at").isNull())
    .drop("_shipped_at_raw","_delivered_at_raw"))

# Dedup delivery_id: prioriza linha com status preenchido e com order_id válido
pedidos = spark.table(f"{SILVER}.pedidos").select("order_id").distinct()
clean = (clean
    .join(pedidos.withColumnRenamed("order_id","_ok"), F.col("order_id")==F.col("_ok"), "left")
    .withColumn("pedido_orfao", F.col("_ok").isNull())
    .drop("_ok"))

score = (F.when(F.col("status_entrega").isNotNull(),2).otherwise(0)
       + F.when(~F.col("pedido_orfao"),1).otherwise(0)
       + F.when(~F.col("data_envio_invalida") & ~F.col("data_entrega_invalida"),1).otherwise(0))
w = W.partitionBy("delivery_id").orderBy(score.desc())
dedup = clean.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

# Métrica derivada
dedup = (dedup
    .withColumn("dias_transito",
                F.datediff("delivered_at","shipped_at")))

write_silver(dedup, "entregas")

# COMMAND ----------

# MAGIC %md ## atendimentos

# COMMAND ----------

raw = spark.table(f"{BRONZE}.atendimento")

clean = (raw
    .withColumn("ticket_id",   upper_trim("ticket_id"))
    .withColumn("order_id",    upper_trim("order_id"))
    .withColumn("event_type",  norm_event_type("event_type"))
    .withColumn("severity",    norm_severity("severity"))
    .withColumn("status_ticket", norm_status_ticket("status"))
    .withColumn("created_at",  parse_ts_multi("created_at"))
    .select("ticket_id","order_id","event_type","severity","status_ticket","created_at"))

pedidos = spark.table(f"{SILVER}.pedidos").select("order_id").distinct()
clean = (clean
    .join(pedidos.withColumnRenamed("order_id","_ok"), F.col("order_id")==F.col("_ok"), "left")
    .withColumn("pedido_orfao", F.col("_ok").isNull())
    .drop("_ok"))

write_silver(clean, "atendimentos")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SILVER}"))
