# Fluxo Completo do Projeto SINASC

## Visão Geral do Pipeline

Este documento explica como os dados brutos do SINASC são transformados até chegarem na tabela fato do banco de dados PostgreSQL, passando por todas as etapas de processamento.

---

## **1. DOWNLOAD - Obtenção dos Dados Brutos**

**Script:** `download_sinasc.py`

**O que faz:**
- Baixa arquivos DBF brutos do DATASUS (dados.gov.br)
- Organiza os arquivos por ano em pastas separadas
- Gera registro de metadados do download

**Entrada:**
- URLs do DATASUS (dados públicos)

**Saída:**
- `data/raw/2013/` ← arquivos DBF de 2013
- `data/raw/2014/` ← arquivos DBF de 2014
- ... (até 2023)
- `data/raw/download_manifest.json` ← registro do download

---

## **2. ETL - Limpeza e Harmonização**

**Script:** `etl_sinasc.py`

**O que faz:**
- Lê os arquivos DBF brutos de `data/raw/`
- Padroniza nomes de colunas (que mudam entre anos)
- Limpa dados, converte tipos (datas, inteiros, strings)
- Trata valores nulos e inconsistências
- Harmoniza códigos categóricos
- Unifica todos os anos em um único CSV

**Entrada:**
- `data/raw/2013/`, `data/raw/2014/`, ... (arquivos DBF)

**Saída:**
- `data/processed/sinasc_harmonized.csv` ← CSV único, limpo e padronizado
- `data/processed/sinasc_harmonized_columns.json` ← mapeamento de colunas
- `data/processed/logs/etl_run_*.json` ← logs de execução

---

## **3. STAGING CSV - Preparação para o Banco**

**Script:** `build_staging_csv.py`

**O que faz:**
- Pega `sinasc_harmonized.csv` do ETL
- Adiciona campos de rastreamento:
  - `run_id` ← identificador único da execução
  - `source_row_number` ← número da linha original
  - `source_zip` ← arquivo de origem
- Prepara o CSV no formato exato esperado pelo PostgreSQL

**Entrada:**
- `data/processed/sinasc_harmonized.csv`

**Saída:**
- `data/processed/stg_sinasc_harmonized.csv` ← pronto para o banco

---

## **4. CRIAÇÃO DO BANCO - Estrutura Inicial**

**Script SQL:** `fase4_sinasc_postgresql.sql`

**O que faz:**
- Cria o schema `sinasc` no PostgreSQL
- Cria as 5 tabelas principais:
  1. **`dim_municipio_ibge`** ← carrega de `data/reference/ibge_municipios.csv`
  2. **`dim_estabelecimento`** ← populada durante o load
  3. **`dim_categoria`** ← populada com códigos hardcoded (sexo, raça/cor, parto, etc.)
  4. **`stg_sinasc_harmonized`** ← carrega o CSV do staging
  5. **`fato_nascimentos`** ← tabela fato particionada por ano (vazia inicialmente)

**Entrada:**
- `data/reference/ibge_municipios.csv` ← municípios IBGE
- `data/processed/stg_sinasc_harmonized.csv` ← dados staging

**Saída:**
- Banco `sinasc_tcc` criado e estruturado
- Dimensões populadas
- Staging carregada
- Fato vazia (aguardando load incremental)

---

## **5. LOAD INCREMENTAL - Carga da Tabela Fato**

**Script SQL:** `load_fato_incremental_2013_2023.sql`

**O que faz:**
- Pega dados de `stg_sinasc_harmonized` (tabela temporária no banco)
- Valida municípios e estabelecimentos usando LEFT JOINs nas dimensões:
  - Se município não existir em `dim_municipio_ibge` → coloca NULL
  - Se estabelecimento não existir em `dim_estabelecimento` → coloca NULL
- Insere na `fato_nascimentos` **ano por ano** (loop de 2013 a 2023)
- Usa `ON CONFLICT DO NOTHING` para evitar duplicação
- Aplica validações e conversões finais (sexo, raça/cor, etc.)

**Entrada:**
- `sinasc.stg_sinasc_harmonized` ← tabela staging no banco
- `sinasc.dim_municipio_ibge` ← para validar municípios
- `sinasc.dim_estabelecimento` ← para validar estabelecimentos

**Saída:**
- `sinasc.fato_nascimentos` ← 30,9 milhões de nascimentos carregados e validados

**Lógica do SQL:**
```sql
FOR cada ano de 2013 a 2023:
    SELECT dados FROM staging WHERE ano = X
    LEFT JOIN dim_municipio_ibge (validar município nascimento)
    LEFT JOIN dim_municipio_ibge (validar município residência)
    LEFT JOIN dim_estabelecimento (validar estabelecimento)
    INSERT INTO fato_nascimentos
    ON CONFLICT DO NOTHING (evita duplicação)
```

---

## **Resumo Visual do Fluxo**

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. DOWNLOAD (Python)                                                │
│    dados.gov.br → data/raw/YYYY/*.dbf                              │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 2. ETL (Python)                                                     │
│    data/raw/*.dbf → limpa/padroniza → sinasc_harmonized.csv       │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 3. STAGING (Python)                                                 │
│    sinasc_harmonized.csv → +rastreamento → stg_sinasc_harmonized.csv│
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 4. CRIAÇÃO BANCO (SQL)                                              │
│    PostgreSQL: cria schema, tabelas, indices, constraints           │
│    Carrega: dimensões + staging                                     │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 5. LOAD INCREMENTAL (SQL)                                           │
│    staging → valida → fato_nascimentos (particionada)               │
│    Resultado: 30,9 milhões de registros validados                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

