# Fase 3 — ETL do SINASC (Semanas 3 e 4)

## Objetivo da fase

Entregar um pipeline de ETL reprodutível para transformar os arquivos brutos do SINASC em dados harmonizados, com validações, integração de referência municipal do IBGE e rastreabilidade por logs/versionamento.

## Script principal

- `scripts/etl_sinasc.py`

## Escopo implementado

- Leitura dos CSVs dentro de ZIP por ano (`data/raw/<ano>/SINASC_<ano>_csv.zip`)
- Limpeza de valores (trim, normalização de marcadores de ausência)
- Padronização de nomes de colunas (uppercase) e harmonização entre anos
- Conversão de tipos derivados (data, inteiros clínicos e demográficos)
- Tratamento de valores faltantes
- Integração com tabela de municípios do IBGE (API oficial com cache local)
- Padronização de códigos de município (`CODMUN*` em formato de 6 dígitos)
- Tratamento e validação do código de estabelecimento (`CODESTAB`)
- Criação de tabela de configuração de categorias (`SEXO`, `RACACOR`, `PARTO`, `GESTACAO`)
- Registro de logs e versionamento da transformação

## Como executar

### 1) Teste rápido (amostra)

```bash
python -u .\scripts\etl_sinasc.py --start-year 2023 --end-year 2023 --max-rows-per-file 5000 --overwrite
```

### 2) Execução completa (intervalo)

```bash
python -u .\scripts\etl_sinasc.py --start-year 2013 --end-year 2023 --overwrite
```

### 3) Execução por padrão (todos os anos encontrados)

```bash
python -u .\scripts\etl_sinasc.py --overwrite
```

## Artefatos de saída

### Dados harmonizados

- `data/processed/sinasc_harmonized.csv`

Contém:

- colunas originais harmonizadas (união entre anos)
- colunas derivadas de padronização/qualidade (ex.: `DTNASC_ISO`, `CODESTAB_VALID`, `CODMUNRES_UF`)

### Metadados de colunas

- `data/processed/sinasc_harmonized_columns.json`

### Tabela de categorias

- `data/processed/reference/category_config.csv`

### Referência IBGE (cache)

- `data/reference/ibge_municipios.csv`

### Logs e versionamento

- `data/processed/logs/etl_latest.json`
- `data/processed/logs/etl_run_<RUN_ID>.json`
- `data/processed/logs/etl_runs.jsonl`

## Regras principais de transformação

- **Harmonização de colunas:** união dos cabeçalhos de todos os anos processados.
- **Datas:** `DTNASC` (`DDMMAAAA`) convertida para `DTNASC_ISO` (`AAAA-MM-DD`) quando válida.
- **Municípios:** códigos normalizados para 6 dígitos e enriquecidos com nome/UF a partir do IBGE.
- **Estabelecimento:** `CODESTAB` normalizado para 7 dígitos, com flag `CODESTAB_VALID`.
- **Tipos numéricos derivados:** `IDADEMAE_INT`, `PESO_INT`, `APGAR1_INT`, `APGAR5_INT`, `CONSPRENAT_INT`.
- **Categorias:** descrição textual derivada para `SEXO`, `RACACOR` e `PARTO`.