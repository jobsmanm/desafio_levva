# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 03 - Gold
# MAGIC
# MAGIC Modelo analítico final (star schema) consumido pelo BI.
# MAGIC
# MAGIC ## Convenção de prefixos
# MAGIC
# MAGIC | Prefixo | Uso | Exemplo |
# MAGIC |---|---|---|
# MAGIC | `sk_` | Surrogate key (SCD2) | `sk_cliente` |
# MAGIC | `cod_` | Código / business key (alfanumérico) | `cod_pedido`, `cod_uf` |
# MAGIC | `nom_` | Nome próprio | `nom_cliente`, `nom_cidade` |
# MAGIC | `des_` | Descrição / categorização | `des_categoria`, `des_segmento` |
# MAGIC | `tip_` | Tipo | `tip_canal`, `tip_modal` |
# MAGIC | `sts_` | Status | `sts_pedido`, `sts_entrega` |
# MAGIC | `flg_` | Boolean | `flg_cancelado`, `flg_atual` |
# MAGIC | `dt_`  | Data / timestamp | `dt_pedido`, `dt_inicio_vigencia` |
# MAGIC | `qtd_` | Quantidade contável | `qtd_itens` |
# MAGIC | `num_` | Número (sequencial / métrica) | `num_item`, `num_ano` |
# MAGIC | `prc_` | Preço unitário | `prc_unitario`, `prc_lista` |
# MAGIC | `val_` | Valor monetário total | `val_total_item`, `val_liquido` |
# MAGIC
# MAGIC ## SCD
# MAGIC
# MAGIC - **SCD2** em `dim_cliente` e `dim_produto` — entidades com `updated_at` na fonte e atributos que afetam análise histórica (segmento, porte, preço de lista).
# MAGIC - **SCD1** (overwrite) nas demais (`dim_canal`, `dim_regiao`, `dim_vendedor`, `dim_data`).
# MAGIC
# MAGIC ## Padrão SCD2 (MERGE idempotente + IDENTITY)
# MAGIC
# MAGIC A cada execução:
# MAGIC 1. Garante a tabela criada via DDL com `sk_*` declarada como `GENERATED ALWAYS AS IDENTITY`.
# MAGIC 2. Lê estado atual da silver e gera `hash_scd` dos atributos versionados.
# MAGIC 3. Compara com a versão `flg_atual=true` da gold via `MERGE INTO`:
# MAGIC    - Hash diferente → **expira** a versão atual (`flg_atual=false`, `dt_fim_vigencia = ontem`).
# MAGIC 4. **Insere** novas versões via `INSERT INTO ... SELECT` sem informar o `sk_*` — o Delta gera o valor automaticamente.
# MAGIC
# MAGIC Primeira execução: gold vazia → todos os registros entram como `flg_atual=true`.
# MAGIC
# MAGIC ## Metadados
# MAGIC
# MAGIC - SCD1: `dt_processamento_gold`
# MAGIC - SCD2: `dt_inicio_vigencia`, `dt_fim_vigencia`, `flg_atual`, `dt_processamento_gold`, `hash_scd` (mais `sk_*` IDENTITY gerada pelo Delta)

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

CATALOG = "workspace"
SILVER  = f"{CATALOG}.desafio_silver"
GOLD    = f"{CATALOG}.desafio_gold"

DT_FIM_ABERTA = F.lit("9999-12-31").cast("date")

def write_gold_scd1(df, name):
    """Overwrite simples para dims SCD1 e fatos."""
    df = df.withColumn("dt_processamento_gold", F.current_timestamp())
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(f"{GOLD}.{name}"))
    print(f"{GOLD}.{name}: {df.count()} linhas")

# COMMAND ----------

# MAGIC %md ## Helper: aplicar SCD2 via MERGE
# MAGIC
# MAGIC Idempotente. Surrogate key gerada pelo Delta via `GENERATED ALWAYS AS IDENTITY`.
# MAGIC
# MAGIC Fluxo:
# MAGIC 1. Se a tabela não existe, cria via DDL com `IDENTITY` no `sk_*`.
# MAGIC 2. Calcula `hash_scd` da silver e compara com versões `flg_atual=true` da gold.
# MAGIC 3. `MERGE`: registros cujo hash mudou → expira versão atual (`flg_atual=false`, `dt_fim_vigencia=ontem`).
# MAGIC 4. `INSERT INTO` (sem informar `sk_*`): insere as novas versões + chaves novas. O Delta gera `sk_*` automaticamente.

# COMMAND ----------

def _spark_type_to_ddl(dt):
    """Converte tipo Spark para string DDL aceita pelo CREATE TABLE."""
    return dt.simpleString()

def apply_scd2(source_df, table_name, biz_key, sk_col, scd_attrs):
    """
    source_df: DataFrame com estado atual (silver) já no schema da gold (sem colunas técnicas SCD).
    table_name: nome da tabela gold (sem catalog/schema).
    biz_key: nome da coluna business key (ex: 'cod_cliente').
    sk_col: nome da surrogate key (gerada via IDENTITY).
    scd_attrs: lista de colunas que disparam nova versão quando mudam.
    """
    full_table = f"{GOLD}.{table_name}"
    today = F.current_date()
    yesterday = F.date_sub(today, 1)

    # Hash dos atributos versionados
    src = source_df.withColumn(
        "hash_scd",
        F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in scd_attrs]), 256)
    )

    # ---- Passo 1: garante tabela com IDENTITY criada via DDL ----
    if not spark.catalog.tableExists(full_table):
        cols_ddl = ",\n    ".join(
            f"`{f.name}` {_spark_type_to_ddl(f.dataType)}" for f in src.schema.fields
        )
        spark.sql(f"""
            CREATE TABLE {full_table} (
                `{sk_col}` BIGINT GENERATED ALWAYS AS IDENTITY,
                {cols_ddl},
                dt_inicio_vigencia DATE,
                dt_fim_vigencia DATE,
                flg_atual BOOLEAN,
                dt_processamento_gold TIMESTAMP
            ) USING DELTA
        """)
        print(f"[{table_name}] tabela criada com IDENTITY({sk_col})")

    # ---- Passo 2: estado atual da gold (versão vigente) ----
    gold_atual = (spark.table(full_table)
                  .filter("flg_atual = true")
                  .select(biz_key, "hash_scd"))

    # ---- Passo 3: classifica registros (mudou / novo / inalterado) ----
    src_vs_gold = (src.alias("s")
        .join(gold_atual.alias("g"), F.col(f"s.{biz_key}") == F.col(f"g.{biz_key}"), "left")
        .select(
            *[F.col(f"s.{c}") for c in src.columns],
            F.col("g.hash_scd").alias("_hash_gold"),
        ))

    mudou      = src_vs_gold.filter("_hash_gold is not null and hash_scd != _hash_gold").drop("_hash_gold")
    novo       = src_vs_gold.filter("_hash_gold is null").drop("_hash_gold")
    inalterado = src_vs_gold.filter("_hash_gold is not null and hash_scd = _hash_gold").count()
    print(f"[{table_name}] mudou: {mudou.count()} | novo: {novo.count()} | inalterado: {inalterado}")

    # ---- Passo 4: MERGE para expirar versões correntes que mudaram ----
    if mudou.count() > 0:
        delta_tbl = DeltaTable.forName(spark, full_table)
        (delta_tbl.alias("t")
            .merge(mudou.select(biz_key).alias("s"),
                   f"t.{biz_key} = s.{biz_key} AND t.flg_atual = true")
            .whenMatchedUpdate(set={
                "flg_atual": F.lit(False),
                "dt_fim_vigencia": yesterday,
                "dt_processamento_gold": F.current_timestamp(),
            })
            .execute())

    # ---- Passo 5: INSERT INTO para gravar novas versões (IDENTITY gera sk_*) ----
    # Regra de vigência:
    #   - chave nova (primeira versão da entidade)        → dt_inicio = '1900-01-01'
    #     (garante que fatos com datas anteriores à carga ainda casem com a versão inicial)
    #   - versão nova de chave existente (mudou atributo) → dt_inicio = hoje
    DT_INICIO_PRIMORDIAL = F.lit("1900-01-01").cast("date")

    def _insert(df, dt_inicio_expr):
        df = (df
            .withColumn("dt_inicio_vigencia", dt_inicio_expr)
            .withColumn("dt_fim_vigencia", DT_FIM_ABERTA)
            .withColumn("flg_atual", F.lit(True))
            .withColumn("dt_processamento_gold", F.current_timestamp()))
        df.createOrReplaceTempView("_tmp_scd2_inserts")
        cols = df.columns
        col_list = ", ".join(f"`{c}`" for c in cols)
        spark.sql(f"""
            INSERT INTO {full_table} ({col_list})
            SELECT {col_list} FROM _tmp_scd2_inserts
        """)

    if novo.count() > 0:
        _insert(novo, DT_INICIO_PRIMORDIAL)
    if mudou.count() > 0:
        _insert(mudou, today)

    total = spark.table(full_table).count()
    atuais = spark.table(full_table).filter("flg_atual=true").count()
    print(f"[{table_name}] total: {total} ({atuais} vigentes + {total-atuais} expiradas)")

# COMMAND ----------

# MAGIC %md ## dim_cliente — SCD2
# MAGIC
# MAGIC Atributos versionados: `nom_cliente`, `des_segmento`, `des_porte`, `nom_cidade`, `cod_uf`, `des_email`, `flg_email_valido`, `sts_cliente`.

# COMMAND ----------

src_cliente = (spark.table(f"{SILVER}.clientes")
    .select(
        F.col("customer_id").alias("cod_cliente"),
        F.col("nome_cliente").alias("nom_cliente"),
        F.col("segmento").alias("_segmento"),
        F.col("porte").alias("_porte"),
        F.col("cidade").alias("_cidade"),
        F.col("uf").alias("cod_uf"),
        F.col("data_cadastro").alias("dt_cadastro"),
        F.col("email").alias("des_email"),
        F.col("email_valido").alias("flg_email_valido"),
        F.col("status").alias("sts_cliente"),
    )
    .withColumn("des_segmento", F.coalesce(F.col("_segmento"), F.lit("Não informado")))
    .withColumn("des_porte",    F.coalesce(F.col("_porte"),    F.lit("Não informado")))
    .withColumn("nom_cidade",   F.coalesce(F.col("_cidade"),   F.lit("Não informado")))
    .drop("_segmento","_porte","_cidade"))

apply_scd2(
    source_df=src_cliente,
    table_name="dim_cliente",
    biz_key="cod_cliente",
    sk_col="sk_cliente",
    scd_attrs=["nom_cliente","des_segmento","des_porte","nom_cidade","cod_uf",
               "des_email","flg_email_valido","sts_cliente"],
)

# COMMAND ----------

# MAGIC %md ## dim_produto — SCD2
# MAGIC
# MAGIC Atributos versionados: `nom_produto`, `des_categoria`, `des_subcategoria`, `des_familia`, `prc_lista`, `sts_produto`.

# COMMAND ----------

src_produto = (spark.table(f"{SILVER}.produtos")
    .select(
        F.col("product_id").alias("cod_produto"),
        F.col("nome_produto").alias("nom_produto"),
        F.col("categoria").alias("_categoria"),
        F.col("subcategoria").alias("_subcategoria"),
        F.col("familia").alias("_familia"),
        F.col("tags").alias("des_tags"),
        F.col("list_price").alias("prc_lista"),
        F.col("moeda").alias("cod_moeda"),
        F.col("status").alias("sts_produto"),
    )
    .withColumn("des_categoria",    F.coalesce(F.col("_categoria"),    F.lit("Não informado")))
    .withColumn("des_subcategoria", F.coalesce(F.col("_subcategoria"), F.lit("Não informado")))
    .withColumn("des_familia",      F.coalesce(F.col("_familia"),      F.lit("Não informado")))
    .drop("_categoria","_subcategoria","_familia"))

apply_scd2(
    source_df=src_produto,
    table_name="dim_produto",
    biz_key="cod_produto",
    sk_col="sk_produto",
    scd_attrs=["nom_produto","des_categoria","des_subcategoria","des_familia",
               "prc_lista","sts_produto"],
)

# COMMAND ----------

# MAGIC %md ## dim_canal — SCD1

# COMMAND ----------

dim_canal = (spark.table(f"{SILVER}.canais")
    .select(
        F.col("canal_id").alias("cod_canal"),
        F.col("nome_canal").alias("nom_canal"),
        F.col("tipo_canal").alias("tip_canal"),
        F.col("ativo").alias("sts_canal"),
    ))

write_gold_scd1(dim_canal, "dim_canal")

# COMMAND ----------

# MAGIC %md ## dim_regiao — SCD1

# COMMAND ----------

dim_regiao = (spark.table(f"{SILVER}.regioes")
    .select(
        F.col("regional_code").alias("cod_regional"),
        F.col("regional_name").alias("nom_regional"),
        F.col("uf_sede").alias("cod_uf_sede"),
        F.col("manager_name").alias("nom_gestor"),
        F.col("ativo").alias("sts_regional"),
    ))

write_gold_scd1(dim_regiao, "dim_regiao")

# COMMAND ----------

# MAGIC %md ## dim_vendedor — SCD1

# COMMAND ----------

dim_vendedor = (spark.table(f"{SILVER}.vendedores")
    .select(
        F.col("seller_id").alias("cod_vendedor"),
        F.col("seller_name").alias("nom_vendedor"),
        F.col("canal_id").alias("_canal"),
        F.col("regional_code").alias("_regional"),
        F.col("hire_date").alias("dt_admissao"),
        F.col("status").alias("sts_vendedor"),
    )
    .withColumn("cod_canal",     F.coalesce(F.col("_canal"),    F.lit("Não informado")))
    .withColumn("cod_regional",  F.coalesce(F.col("_regional"), F.lit("Não informado")))
    .drop("_canal","_regional"))

write_gold_scd1(dim_vendedor, "dim_vendedor")

# COMMAND ----------

# MAGIC %md ## dim_data

# COMMAND ----------

ped = spark.table(f"{SILVER}.pedidos").select(F.col("order_date").alias("d"))
ped = ped.union(spark.table(f"{SILVER}.pedidos").select(F.col("promised_date").alias("d")))
ped = ped.union(spark.table(f"{SILVER}.entregas").select(F.col("shipped_at").cast("date").alias("d")))
ped = ped.union(spark.table(f"{SILVER}.entregas").select(F.col("delivered_at").cast("date").alias("d")))
ped = ped.union(spark.table(f"{SILVER}.atendimentos").select(F.col("created_at").cast("date").alias("d")))
ped = ped.filter("d is not null")

bounds = ped.agg(F.min("d").alias("min_d"), F.max("d").alias("max_d")).collect()[0]
min_d, max_d = bounds["min_d"], bounds["max_d"]

dim_data = (spark.sql(f"""
    SELECT explode(sequence(date'{min_d}', date'{max_d}', interval 1 day)) AS dt_referencia
""")
    .withColumn("num_ano",        F.year("dt_referencia"))
    .withColumn("num_mes",        F.month("dt_referencia"))
    .withColumn("num_dia",        F.dayofmonth("dt_referencia"))
    .withColumn("num_trimestre",  F.quarter("dt_referencia"))
    .withColumn("cod_ano_mes",    F.date_format("dt_referencia","yyyy-MM"))
    .withColumn("nom_dia_semana", F.date_format("dt_referencia","EEEE"))
    .withColumn("flg_fim_semana", F.dayofweek("dt_referencia").isin(1,7)))

write_gold_scd1(dim_data, "dim_data")

# COMMAND ----------

# MAGIC %md ## Lookup SCD2 (helper para fatos)
# MAGIC
# MAGIC Resolve `sk_*` da dim SCD2 buscando a versão vigente na data do evento.

# COMMAND ----------

def lookup_scd2_sk(fato_df, dim_table, biz_key, sk_col, date_col):
    """Resolve sk_* da dim SCD2 buscando a versão vigente em date_col.
    Retorna todas as colunas de fato_df + sk_col (sem duplicar biz_key)."""
    dim = (spark.table(f"{GOLD}.{dim_table}")
           .select(biz_key, sk_col, "dt_inicio_vigencia", "dt_fim_vigencia"))

    joined = (fato_df.alias("f")
        .join(dim.alias("d"),
              (F.col(f"f.{biz_key}") == F.col(f"d.{biz_key}")) &
              (F.col(f"f.{date_col}") >= F.col("d.dt_inicio_vigencia")) &
              (F.col(f"f.{date_col}") <= F.col("d.dt_fim_vigencia")),
              "left"))

    # Mantém apenas as colunas de f + sk_col do lado d (descarta biz_key duplicado e datas de vigência)
    return joined.select(
        *[F.col(f"f.{c}") for c in fato_df.columns],
        F.col(f"d.{sk_col}").alias(sk_col),
    )

# COMMAND ----------

# MAGIC %md ## fato_pedido_cabecalho

# COMMAND ----------

agg_itens = (spark.table(f"{SILVER}.pedidos_itens")
    .groupBy("order_id")
    .agg(
        F.count("*").alias("qtd_itens"),
        F.sum(F.when(F.col("item_status")=="cancelado", 0).otherwise(F.col("quantity"))).alias("qtd_total_unidades"),
        F.sum(F.when(F.col("item_status")=="cancelado", 0).otherwise(F.col("total_item"))).alias("val_itens_validos"),
    ))

cab = (spark.table(f"{SILVER}.pedidos")
    .select(
        F.col("order_id").alias("cod_pedido"),
        F.col("customer_id").alias("cod_cliente"),
        F.col("seller_id").alias("cod_vendedor"),
        F.col("order_date").alias("dt_pedido"),
        F.col("promised_date").alias("dt_promessa"),
        F.col("status_pedido").alias("sts_pedido"),
        F.col("gross_amount").alias("val_bruto"),
        F.col("discount_amount").alias("val_desconto"),
        F.col("net_amount").alias("val_liquido"),
        F.col("payment_priority").alias("des_prioridade_pagamento"),
        F.col("payment_source").alias("cod_origem_pedido"),
        F.col("last_update").alias("dt_ultima_atualizacao"),
    )
    .withColumn("flg_cancelado", F.col("sts_pedido") == "cancelado"))

vend = (spark.table(f"{SILVER}.vendedores")
        .select(F.col("seller_id").alias("cod_vendedor"),
                F.col("canal_id").alias("cod_canal"),
                F.col("regional_code").alias("cod_regional")))

fato_ped = (cab
    .join(vend, "cod_vendedor", "left")
    .join(agg_itens.withColumnRenamed("order_id","cod_pedido"), "cod_pedido", "left"))

fato_ped = lookup_scd2_sk(fato_ped, "dim_cliente",
                          biz_key="cod_cliente", sk_col="sk_cliente", date_col="dt_pedido")

fato_ped = fato_ped.select(
    "cod_pedido","sk_cliente","cod_cliente","cod_vendedor","cod_canal","cod_regional",
    "dt_pedido","dt_promessa",
    "sts_pedido","flg_cancelado",
    "val_bruto","val_desconto","val_liquido",
    "qtd_itens","qtd_total_unidades","val_itens_validos",
    "des_prioridade_pagamento","cod_origem_pedido","dt_ultima_atualizacao",
)

write_gold_scd1(fato_ped, "fato_pedido_cabecalho")

# COMMAND ----------

# MAGIC %md ## fato_pedido_item

# COMMAND ----------

itens = (spark.table(f"{SILVER}.pedidos_itens")
    .select(
        F.col("order_id").alias("cod_pedido"),
        F.col("item_seq").alias("num_item"),
        F.col("product_id").alias("cod_produto"),
        F.col("quantity").alias("qtd_item"),
        F.col("unit_price").alias("prc_unitario"),
        F.col("total_item").alias("val_total_item"),
        F.col("item_status").alias("sts_item"),
        F.col("quantity_invalida").alias("_qty_invalida"),
        F.col("total_divergente").alias("_total_divergente"),
    ))

cab_ctx = (spark.table(f"{GOLD}.fato_pedido_cabecalho")
    .select("cod_pedido","cod_cliente","sk_cliente","cod_vendedor","cod_canal","cod_regional",
            "dt_pedido","dt_promessa","sts_pedido","flg_cancelado"))

fato_item = itens.join(cab_ctx, "cod_pedido", "left")

fato_item = lookup_scd2_sk(fato_item, "dim_produto",
                           biz_key="cod_produto", sk_col="sk_produto", date_col="dt_pedido")

fato_item = (fato_item
    .withColumn("flg_cancelado_item",
                (F.col("sts_item") == "cancelado") | F.col("flg_cancelado"))
    .withColumn("_item_valido",
                ~F.coalesce(F.col("_qty_invalida"), F.lit(False)) &
                ~F.coalesce(F.col("_total_divergente"), F.lit(False)))
    .withColumn("val_receita_item",
                F.when(F.col("flg_cancelado_item") | ~F.col("_item_valido"), F.lit(0))
                 .otherwise(F.col("val_total_item"))))

fato_item = fato_item.select(
    "cod_pedido","num_item",
    "sk_produto","cod_produto",
    "sk_cliente","cod_cliente",
    "cod_vendedor","cod_canal","cod_regional",
    "dt_pedido","dt_promessa",
    "qtd_item","prc_unitario","val_total_item","val_receita_item",
    "sts_pedido","sts_item","flg_cancelado_item",
)

write_gold_scd1(fato_item, "fato_pedido_item")

# COMMAND ----------

# MAGIC %md ## fato_entrega

# COMMAND ----------

ent = (spark.table(f"{SILVER}.entregas")
    .select(
        F.col("delivery_id").alias("cod_entrega"),
        F.col("order_id").alias("cod_pedido"),
        F.col("transportadora").alias("_transportadora"),
        F.col("modal").alias("_modal"),
        F.col("status_entrega").alias("sts_entrega"),
        F.col("shipped_at").alias("dt_envio"),
        F.col("delivered_at").alias("dt_entrega"),
        F.col("uf_destino").alias("cod_uf_destino"),
        F.col("cidade_destino").alias("nom_cidade_destino"),
        F.col("custo_frete").alias("val_frete"),
        F.col("dias_transito").alias("qtd_dias_transito"),
    )
    .withColumn("nom_transportadora", F.coalesce(F.col("_transportadora"), F.lit("Não informado")))
    .withColumn("tip_modal",          F.coalesce(F.col("_modal"),          F.lit("Não informado")))
    .drop("_transportadora","_modal"))

cab_ctx = (spark.table(f"{GOLD}.fato_pedido_cabecalho")
    .select("cod_pedido","sk_cliente","cod_cliente","cod_vendedor","dt_pedido","dt_promessa"))

fato_ent = (ent.join(cab_ctx, "cod_pedido", "left")
    .withColumn("qtd_dias_vs_promessa",
                F.datediff(F.col("dt_entrega").cast("date"), F.col("dt_promessa")))
    .withColumn("flg_no_prazo",
                F.col("dt_entrega").isNotNull() & F.col("dt_promessa").isNotNull() &
                (F.col("dt_entrega").cast("date") <= F.col("dt_promessa")))
    .withColumn("flg_com_atraso",
                F.col("dt_entrega").isNotNull() & F.col("dt_promessa").isNotNull() &
                (F.col("dt_entrega").cast("date") > F.col("dt_promessa")))
    .select(
        "cod_entrega","cod_pedido","sk_cliente","cod_cliente","cod_vendedor",
        "nom_transportadora","tip_modal","sts_entrega",
        "dt_envio","dt_entrega","dt_promessa",
        "qtd_dias_transito","qtd_dias_vs_promessa",
        "flg_no_prazo","flg_com_atraso",
        "cod_uf_destino","nom_cidade_destino","val_frete",
    ))

write_gold_scd1(fato_ent, "fato_entrega")

# COMMAND ----------

# MAGIC %md ## fato_atendimento

# COMMAND ----------

at = (spark.table(f"{SILVER}.atendimentos")
    .select(
        F.col("ticket_id").alias("cod_ticket"),
        F.col("order_id").alias("cod_pedido"),
        F.col("event_type").alias("_evento"),
        F.col("severity").alias("_severidade"),
        F.col("status_ticket").alias("sts_ticket"),
        F.col("created_at").alias("dt_abertura"),
    )
    .withColumn("tip_evento",     F.coalesce(F.col("_evento"),     F.lit("Não informado")))
    .withColumn("des_severidade", F.coalesce(F.col("_severidade"), F.lit("Não informado")))
    .drop("_evento","_severidade"))

cab_ctx = (spark.table(f"{GOLD}.fato_pedido_cabecalho")
    .select("cod_pedido","sk_cliente","cod_cliente","cod_vendedor","cod_canal","cod_regional"))

fato_at = (at.join(cab_ctx, "cod_pedido", "left")
    .select(
        "cod_ticket","cod_pedido","sk_cliente","cod_cliente","cod_vendedor","cod_canal","cod_regional",
        "tip_evento","des_severidade","sts_ticket","dt_abertura",
    ))

write_gold_scd1(fato_at, "fato_atendimento")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {GOLD}"))
