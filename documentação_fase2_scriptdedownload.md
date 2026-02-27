# Fase 2 — Download Automatizado dos Dados do SINASC

## 1) Objetivo da fase

Implementar a ingestão automatizada dos arquivos públicos do SINASC, eliminando downloads manuais e organizando os dados brutos para a próxima etapa do TCC (pipeline ETL).

Esta fase entrega:

- Download por intervalo de anos
- Organização em pastas por ano
- Manifesto com rastreabilidade da execução
- Reexecução segura (idempotente)

## 2) Fonte dos dados

Os arquivos são obtidos da base pública do DATASUS/OpenDataSUS no S3:

`https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv`

Padrão de arquivo por ano:

`SINASC_<ANO>_csv.zip`

Exemplo:

`https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2023_csv.zip`

## 3) Script implementado

Arquivo:

- `scripts/download_sinasc.py`

Funcionalidades implementadas no script:

- Construção automática da URL por ano
- Download via HTTPS com `requests` (streaming)
- Escrita do arquivo em `data/raw/<ano>/`
- Geração do manifesto `download_manifest.json`
- Controle de reexecução (`skip` se o arquivo já existir com tamanho > 0)

## 4) Parâmetros de execução

Parâmetros obrigatórios:

- `--start-year` (ano inicial)
- `--end-year` (ano final)

Parâmetro opcional:

- `--out` (diretório de saída; padrão: `data/raw`)

Exemplos:

```bash
python -u .\scripts\download_sinasc.py --start-year 2023 --end-year 2023
python -u .\scripts\download_sinasc.py --start-year 2020 --end-year 2023
python -u .\scripts\download_sinasc.py --start-year 1996 --end-year 2023
python -u .\scripts\download_sinasc.py --start-year 1996 --end-year 2005
```

## 5) Estrutura de saída

Após a execução, a estrutura esperada é:

```text
data/
└── raw/
        ├── 1996/
        │   └── SINASC_1996_csv.zip
        ├── 1997/
        │   └── SINASC_1997_csv.zip
        ├── ...
        ├── 2023/
        │   └── SINASC_2023_csv.zip
        └── download_manifest.json
```

## 6) Status de processamento

O script registra um status por arquivo:

- `ok` — download concluído
- `skip` — arquivo já existia e não foi baixado novamente
- `error_http_<código>` — erro HTTP (ex.: `error_http_404`)
- `error_empty` — arquivo baixado com tamanho zero
- `error_connecttimeout` — timeout de conexão
- `error_readtimeout` — timeout de leitura
- `error_connection` — falha de conexão
- `error_<Exception>` — erro inesperado

## 7) Manifesto de rastreabilidade

O arquivo `download_manifest.json` registra:

- `source` (URL base da fonte)
- `start_year` e `end_year`
- lista `files`, contendo para cada ano:
    - `year`
    - `url`
    - `saved_as`
    - `status`

Esse artefato suporta auditoria, transparência metodológica e reprodutibilidade científica.

## 8) Contribuição da fase para o TCC

A Fase 2 estabelece a camada de ingestão do projeto, coerente com o objetivo do TCC de construir uma infraestrutura científica para o SINASC:

- elimina dependência de download manual
- padroniza entrada de dados para ETL
- cria base rastreável para etapas de limpeza e modelagem

Com isso, o projeto avança para a Fase 3 (ETL), mantendo foco em engenharia de dados reprodutível e não em modelagem de ML.
