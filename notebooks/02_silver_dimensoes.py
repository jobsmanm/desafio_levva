# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 02 - Silver - Dimensões
# MAGIC
# MAGIC bronze → silver para entidades cadastrais: clientes, produtos, canais, regioes, vendedores.
# MAGIC
# MAGIC Transformações:
# MAGIC - Normalização de chaves (UPPER, TRIM)
# MAGIC - Normalização de domínios categóricos (status, porte, segmento, etc.)
# MAGIC - Parse de datas multi-formato
# MAGIC - Normalização de UF
# MAGIC - Deduplicação por chave de negócio (mantém mais recente por `updated_at`)
# MAGIC - Achatamento de JSONs aninhados
# MAGIC - Tabela de quarentena para registros inválidos

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import StringType

# os parses usam try_to_* abaixo, que retornam null em vez de exceção.
try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
except Exception as e:
    print(f"Config ignorada (esperado em serverless): {e}")

CATALOG = "workspace"
BRONZE  = f"{CATALOG}.desafio_bronze"
SILVER  = f"{CATALOG}.desafio_silver"

# ----- Helpers reutilizáveis -----
def upper_trim(col):
    return F.upper(F.trim(col))

def norm_status_ativo(col):
    """sim/Sim/SIM/1/ativo/Ativo/ATIVO → 'ativo'; nao/0/inativo → 'inativo'; resto → null"""
    c = F.lower(F.trim(col))
    return (F.when(c.isin("sim","1","ativo","true"),  F.lit("ativo"))
             .when(c.isin("nao","não","0","inativo","false"), F.lit("inativo"))
             .otherwise(F.lit(None)))

def parse_date_multi(col):
    """Tenta múltiplos formatos com try_to_timestamp (tolera strings inválidas → null)."""
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

# Mapa de UFs (todas as variações encontradas nas fontes + Algumas outras que poderiam ser usadas no futuro)
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
    # Se for null, mantém null; se for string desconhecida, mantém valor original em upper para depuração
    return expr.otherwise(F.when(c.isNull(), F.lit(None)).otherwise(F.upper(F.trim(col))))

REGIONAL_MAP = {"s":"S","sul":"S","n":"N","ne":"NE","se":"SE","co":"CO"}
def norm_regional(col):
    c = F.lower(F.trim(col))
    expr = None
    for k,v in REGIONAL_MAP.items():
        cond = (c == F.lit(k))
        expr = F.when(cond, F.lit(v)) if expr is None else expr.when(cond, F.lit(v))
    return expr.otherwise(F.when(c.isNull(), F.lit(None)).otherwise(F.upper(F.trim(col))))

def write_silver(df, name):
    df = df.withColumn("dt_processamento_silver", F.current_timestamp())
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(f"{SILVER}.{name}"))
    print(f"{SILVER}.{name}: {df.count()} linhas")

# COMMAND ----------

# MAGIC %md ## regioes
# MAGIC - Normaliza `regional_code` (`sul`→`S`)
# MAGIC - Normaliza UF do estado-sede
# MAGIC - Descarta `XX` (registro sem nome/gestor, marcado como inativo na fonte)
# MAGIC - Dedup por `regional_code` priorizando registro com mais campos preenchidos

# COMMAND ----------

raw = spark.table(f"{BRONZE}.regioes")

clean = (raw
    .withColumn("regional_code", norm_regional("regional_code"))
    .withColumn("regional_name", F.initcap(F.trim("regional_name")))
    .withColumn("uf_sede", norm_uf("state"))
    .withColumn("manager_name", F.trim("manager_name"))
    .withColumn("ativo", norm_status_ativo("active_flag"))
    .filter((F.col("regional_code") != "XX") & F.col("regional_code").isNotNull())
    .select("regional_code","regional_name","uf_sede","manager_name","ativo"))

score = (F.when(F.col("regional_name").isNotNull(),1).otherwise(0)
       + F.when(F.col("uf_sede").isNotNull(),1).otherwise(0))
w = W.partitionBy("regional_code").orderBy(score.desc())
dim = clean.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

write_silver(dim, "regioes")

# COMMAND ----------

# MAGIC %md ## canais
# MAGIC - Normaliza `id_canal` (UPPER) e `ativo`
# MAGIC - Descarta `CH06` (sem nome — registro inutilizável)
# MAGIC - Dedup `CH05` (duplicado conflitante) mantendo registro com `ativo` preenchido

# COMMAND ----------

raw = spark.table(f"{BRONZE}.canais")

clean = (raw
    .withColumn("canal_id", upper_trim("id_canal"))
    .withColumn("nome_canal", F.initcap(F.trim("nome_canal")))
    .withColumn("tipo_canal", F.initcap(F.lower(F.trim("tipo_canal"))))
    .withColumn("ativo", norm_status_ativo("ativo"))
    .filter(F.col("nome_canal").isNotNull())
    .select("canal_id","nome_canal","tipo_canal","ativo"))

score = F.when(F.col("ativo").isNotNull(),1).otherwise(0)
w = W.partitionBy("canal_id").orderBy(score.desc(), F.col("nome_canal").asc())
dim = clean.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

write_silver(dim, "canais")

# COMMAND ----------

# MAGIC %md ## vendedores
# MAGIC - Normaliza `seller_id`, `regional_code`, `canal_id`, `status`
# MAGIC - Parse `hire_date` multi-formato
# MAGIC - Dedup: V004/V008 duplicados — mantém primeira ocorrência (sem updated_at na fonte)

# COMMAND ----------

raw = spark.table(f"{BRONZE}.vendedores")

clean = (raw
    .withColumn("seller_id", upper_trim("seller_id"))
    .withColumn("seller_name", F.trim("seller_name"))
    .withColumn("canal_id", upper_trim("canal_id"))
    .withColumn("regional_code", norm_regional("regional_code"))
    .withColumn("hire_date", parse_date_multi("hire_date"))
    .withColumn("status", norm_status_ativo("status"))
    .select("seller_id","seller_name","canal_id","regional_code","hire_date","status"))

# Marca chave órfã contra dim_canal e dim_regiao
canais = spark.table(f"{SILVER}.canais").select("canal_id").distinct()
regioes = spark.table(f"{SILVER}.regioes").select("regional_code").distinct()

clean = (clean
    .join(canais.withColumnRenamed("canal_id","_ck"), F.col("canal_id")==F.col("_ck"), "left")
    .withColumn("canal_orfao", F.col("canal_id").isNotNull() & F.col("_ck").isNull())
    .drop("_ck")
    .join(regioes.withColumnRenamed("regional_code","_rk"), F.col("regional_code")==F.col("_rk"), "left")
    .withColumn("regiao_orfa", F.col("regional_code").isNotNull() & F.col("_rk").isNull())
    .drop("_rk"))

w = W.partitionBy("seller_id").orderBy(F.col("status").asc_nulls_last())
dim = clean.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

write_silver(dim, "vendedores")

# COMMAND ----------

# MAGIC %md ## clientes
# MAGIC - Normaliza `customer_id` (UPPER), `porte`, `status`, `segmento`
# MAGIC - Normaliza UF (mapa)
# MAGIC - Parse `data_cadastro` e `updated_at` multi-formato
# MAGIC - Valida email (regex simples)
# MAGIC - Dedup por `customer_id` mantendo maior `updated_at`

# COMMAND ----------

raw = spark.table(f"{BRONZE}.clientes")

clean = (raw
    .withColumn("customer_id", upper_trim("customer_id"))
    .withColumn("nome_cliente", F.trim("nome_cliente"))
    .withColumn("segmento", F.initcap(F.trim("segmento")))
    .withColumn("porte", F.initcap(F.lower(F.trim("porte"))))
    .withColumn("cidade", F.trim("cidade"))
    .withColumn("uf", norm_uf("estado"))
    .withColumn("data_cadastro", parse_date_multi("data_cadastro"))
    .withColumn("email", F.trim("email"))
    .withColumn("email_valido", F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
    .withColumn("status", norm_status_ativo("status_cliente"))
    .withColumn("updated_at", parse_ts_multi("updated_at"))
    .select("customer_id","nome_cliente","segmento","porte","cidade","uf",
            "data_cadastro","email","email_valido","status","updated_at"))

w = W.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())
dim = clean.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

write_silver(dim, "clientes")

# COMMAND ----------

# MAGIC %md ## produtos
# MAGIC - Achata JSON aninhado (product, pricing, attributes)
# MAGIC - Normaliza status e categoria
# MAGIC - Dedup por `product_id` mantendo maior `updated_at` (P0006 tem versão revisada)

# COMMAND ----------

raw = spark.table(f"{BRONZE}.produtos")

clean = (raw
    .select(
        F.upper(F.col("product.product_id")).alias("product_id"),
        F.trim(F.col("product.name")).alias("nome_produto"),
        F.initcap(F.trim(F.col("product.category"))).alias("categoria"),
        F.initcap(F.trim(F.col("product.subcategory"))).alias("subcategoria"),
        norm_status_ativo(F.col("product.status")).alias("status"),
        F.col("pricing.list_price").cast("decimal(18,4)").alias("list_price"),
        F.col("pricing.currency").alias("moeda"),
        F.col("attributes.family").alias("familia"),
        F.col("attributes.tags").alias("tags"),
        parse_ts_multi(F.col("updated_at").cast("string")).alias("updated_at"),
    ))

w = W.partitionBy("product_id").orderBy(F.col("updated_at").desc_nulls_last())
dim = clean.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

write_silver(dim, "produtos")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SILVER}"))
