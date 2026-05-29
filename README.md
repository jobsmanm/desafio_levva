# Desafio Data Engineer

Solução em PySpark + Delta no Databricks Free Edition (sucessor do Community Edition, aposentado em 01/2026).

## Estrutura

```
desafio/
├── notebooks/
│   ├── 00_setup.py             # cria catalog, schemas, volume; upload das fontes
│   ├── 01_bronze.py            # raw → bronze (1 tabela por fonte)
│   ├── 02_silver_dimensoes.py  # clientes, produtos, canais, regioes, vendedores
│   ├── 02_silver_eventos.py    # pedidos (cabecalho+itens), entregas, atendimento
│   ├── 03_gold.py              # dim_* + fato_*
│   └── 98_reset.py             # Realiza a limpeza do projeto (Tabelas de todas as camadas)
│   └── 99_validacoes.py        # checagens de qualidade e contagens
└── README.md
```

## Como rodar no Databricks Free Edition

1. Criar conta em https://www.databricks.com/learn/free-edition.
2. Em **Catalog**, criar volume:
   - Catalog: `workspace` (default)
   - Schema: criar `desafio` (o notebook `00_setup` faz isso)
   - Volume: `raw`
3. Subir os 9 arquivos de `sources/` para `/Volumes/workspace/desafio/raw/`
   (interface Catalog → Volume → Upload).
4. Importar a pasta `notebooks/` no workspace.
5. Anexar qualquer notebook ao **Serverless** compute (única opção no Free Edition).
6. Executar na ordem: `00 → 01 → 02 → 03 → 99`.

## Camadas e metadados

| Camada | Schema | Conteúdo | Metadados |
|---|---|---|---|
| Bronze | `workspace.desafio_bronze` | Cópia da fonte, schema permissivo | `dt_ingestao`, `nom_arquivo_origem` |
| Silver | `workspace.desafio_silver` | Estado atual deduplicado e normalizado, tipado; nomes neutros de entidade; flags técnicas de qualidade | `dt_processamento_silver` |
| Gold | `workspace.desafio_gold` | Star schema (modelagem dimensional) com prefixos padronizados; dimensões SCD1 ou SCD2 | SCD1: `dt_processamento_gold`<br>SCD2: `dt_inicio_vigencia`, `dt_fim_vigencia`, `flg_atual`, `dt_processamento_gold`, `hash_scd` |

### Nomenclatura de tabelas

A distinção dimensão/fato é uma decisão de modelagem dimensional e só aparece na **gold**. A **silver** usa nomes neutros de entidade (próximos da origem conformada):

| Silver | Gold |
|---|---|
| `clientes` | `dim_cliente` (SCD2) |
| `produtos` | `dim_produto` (SCD2) |
| `canais` | `dim_canal` |
| `regioes` | `dim_regiao` |
| `vendedores` | `dim_vendedor` |
| `pedidos` | `fato_pedido_cabecalho` |
| `pedidos_itens` | `fato_pedido_item` |
| `entregas` | `fato_entrega` |
| `atendimentos` | `fato_atendimento` |

## Estratégia de qualidade

Princípio: **flags de qualidade técnica vivem na silver; gold só carrega flags de negócio**.

- Bronze é raw, não filtra nem sinaliza.
- Silver é a camada de conformidade: registros com problema continuam lá com flags (`*_invalida`, `*_orfao`, `email_valido`). Quem precisar auditar, consulta silver.
- Gold serve o analista de BI: não exige conhecer `total_divergente`. Itens inválidos entram com `val_receita_item = 0`; entregas com data inválida entram com `qtd_dias_transito = null`. Tratamento já aplicado.
- Registros descartados são apenas os irrecuperáveis: `regional_code='XX'` (sem nome/gestor, marcado inativo) e `CH06` (canal sem nome).

## SCD (Slowly Changing Dimensions)

- **SCD2** em `dim_cliente` e `dim_produto`: entidades cujos atributos afetam análise histórica (segmento, porte, preço de lista). Implementação via `MERGE INTO` Delta idempotente — primeira execução cria a tabela via DDL com `sk_*` declarada como `GENERATED ALWAYS AS IDENTITY` e carrega tudo como `flg_atual=true`; execuções subsequentes detectam mudanças via hash de atributos, expiram a versão anterior com `MERGE` e inserem a nova com `INSERT INTO` (o Delta gera a surrogate key automaticamente).
- **SCD1** (overwrite) nas demais (`dim_canal`, `dim_regiao`, `dim_vendedor`, `dim_data`): fontes não trazem histórico que justifique versionamento.

Fatos resolvem `sk_cliente` e `sk_produto` por lookup na dim SCD2 pela `dt_pedido` (versão vigente na data do evento).

## Padrão de nomenclatura (apenas na gold)

| Prefixo | Uso |
|---|---|
| `sk_` | Surrogate key SCD2 |
| `cod_` | Código / business key |
| `nom_` | Nome próprio |
| `des_` | Descrição / categorização |
| `tip_` | Tipo |
| `sts_` | Status |
| `flg_` | Boolean |
| `dt_`  | Data / timestamp |
| `qtd_` | Quantidade contável |
| `num_` | Número (sequencial / métrica) |
| `prc_` | Preço unitário |
| `val_` | Valor monetário total |

## Modelo Gold (star schema)

**Dimensões**
- `dim_cliente` — grão: 1 linha por `customer_id`
- `dim_produto` — grão: 1 linha por `product_id`
- `dim_canal` — grão: 1 linha por `canal_id`
- `dim_regiao` — grão: 1 linha por `regional_code`
- `dim_vendedor` — grão: 1 linha por `seller_id`
- `dim_data` — grão: 1 linha por dia (gerada)

**Fatos**
- `fato_pedido_item` — grão: 1 linha por (`order_id`, `item_seq`). Base para receita por produto/categoria.
- `fato_pedido_cabecalho` — grão: 1 linha por `order_id`. Base para ticket médio, taxa de cancelamento.
- `fato_entrega` — grão: 1 linha por `delivery_id`. Base para taxa de atraso, custo de frete.
- `fato_atendimento` — grão: 1 linha por `ticket_id`. Base para volume de ocorrências por pedido.

## Premissas-chave (consolidadas)

- IDs (`customer_id`, `order_id`, `product_id`, `seller_id`, `regional_code`, `canal_id`) são normalizados em **UPPERCASE** para chave canônica.
- Quando há duplicidade por chave de negócio, mantém-se o registro com `updated_at` mais recente (ou `last_update`). Se não houver, considera-se o último arquivo ingerido.
- Estados (UF) são normalizados para a sigla de 2 letras a partir de mapa explícito.
- Datas em formatos mistos são parseadas por tentativa em cascata (`YYYY-MM-DD`, `YYYY/MM/DD`, `DD/MM/YYYY`, `DD/MM/YYYY HH:mm`, ISO 8601). Datas que falham todos os parsers viram `null` e o registro vai para quarentena se a coluna for chave de evento.
- Registros com `quantity <= 0` ou `total_item < 0` são marcados como `item_invalido = true` mas mantidos para auditoria (não entram no cálculo de receita).
- `O99999` referenciado por entrega `D00004` é órfão (não existe em pedidos) → entrega vai para quarentena.
- Códigos de canal/vendedor que não existem no cadastro são mantidos com chave estrangeira nula e flag `chave_orfa`.
