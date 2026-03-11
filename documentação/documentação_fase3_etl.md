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

---

#  Verificacao de Integridade do ETL SINASC

Esta parte explica **somente** a parte de verificacao da limpeza/ETL, com foco em confirmar se houve (ou nao) perda de dados.

## Como a limpeza do ETL foi feita

Script principal: `scripts/etl_sinasc.py`

Regras aplicadas:
- leitura dos ZIPs anuais em `data/raw/<ano>/SINASC_<ano>_csv.zip`;
- harmonizacao de colunas (uniao de cabecalhos entre anos);
- limpeza de valores com `trim` e normalizacao de marcadores de ausencia (`NA`, `NULL`, `IGN`, `SEM INFORMACAO`, etc. -> vazio);
- padronizacao de colunas e criacao de campos derivados;
- conversao de tipos com regras de faixa valida (ex.: APGAR entre 0 e 10);
- enriquecimento de municipio/UF via IBGE;
- escrita em `data/processed/sinasc_harmonized.csv`;
- registro de log em `data/processed/logs/etl_latest.json`.

Importante:
- o ETL **nao remove linhas**;
- quando valor e invalido para conversao, o campo derivado fica vazio, mas o registro continua no dataset.

## Verificacao: antes e depois (perda de dados)

Fonte das evidencias:
- `data/processed/logs/etl_latest.json`
- `data/processed/logs/integrity_report_latest.json`

Resultado consolidado:
- linhas brutas (ZIPs): **30.983.111**
- linhas harmonizadas (CSV final): **30.983.111**
- diferenca: **0**
- correspondencia exata: **true**

Conclusao:
- **Nao houve perda de dados por exclusao de linhas no ETL.**

## Verificacao de nulos e vazios na tabela original

Tabela original = dados brutos lidos dos ZIPs (`raw_stats.missing_selected_fields`).

Principais contagens de missing (marcadores + vazios):
- `DTNASC`: 0
- `IDADEMAE`: 369
- `PESO`: 10.345
- `APGAR1`: 599.930
- `APGAR5`: 600.415
- `CONSPRENAT`: 3.429.231
- `RACACOR`: 992.484
- `PARTO`: 27.056
- `CODESTAB`: 293.655

Leitura tecnica:
- esses missing ja existem no dado de origem;
- portanto, nao indicam perda causada pelo ETL.

## Quantidade de missing antes/depois do tratamento

Comparacao baseada em `harmonized_stats.before_after_missing`:

| Campo bruto | Campo tratado | Missing antes | Missing depois | Delta |
|---|---|---:|---:|---:|
| DTNASC | DTNASC_ISO | 0 | 0 | 0 |
| IDADEMAE | IDADEMAE_INT | 369 | 675 | +306 |
| PESO | PESO_INT | 10.345 | 10.351 | +6 |
| APGAR1 | APGAR1_INT | 599.930 | 622.761 | +22.831 |
| APGAR5 | APGAR5_INT | 600.415 | 618.135 | +17.720 |
| CONSPRENAT | CONSPRENAT_INT | 3.429.231 | 3.429.231 | 0 |

Interpretacao:
- o aumento no campo tratado ocorre porque valores fora da regra de validade viram vazio no campo derivado;
- isso e controle de qualidade do valor, **nao exclusao de linha**.

## Verificacao do site para o local (download)

Validacao em `download_check` do arquivo `data/processed/logs/integrity_report_latest.json`:
- `total_manifest_files`: 11
- `ok_or_skip_files`: 11
- `missing_local_files`: 0
- `zero_size_local_files`: 0
- `remote_size_mismatch_count`: 0

Checks aplicados:
- arquivo listado no manifesto existe localmente;
- tamanho local > 0;
- integridade ZIP (`testzip`) ok;
- comparacao de tamanho remoto (`Content-Length`) vs arquivo local.

Conclusao:
- **Nao houve perda no trajeto site -> local** para os arquivos analisados.

## Reproducao da verificacao

Comando usado:

```powershell
& "c:/Users/fjmen/OneDrive/Documentos/IFSP -- ADS/TCC/TCC/.venv/Scripts/python.exe" scripts/verify_integrity.py --check-remote-size
```

Saida gerada:
- `data/processed/logs/integrity_report_latest.json`