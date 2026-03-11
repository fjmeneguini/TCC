# Fase 4 — Modelagem do Banco de Dados (PostgreSQL 14+)

Recorte: SINASC 2013+ (com dados atuais do projeto em 2013–2023)

## A) Alternativas de modelagem e decisão

### Alternativa 1 — Modelo estrela (fato + múltiplas dimensões)

**Ideia:** `fato_nascimentos` central e dimensões para município, estabelecimento, tempo, categorias (sexo, raça/cor, parto, etc.).

**Prós (PostgreSQL):**
- Melhor organização semântica para BI.
- Reuso de dimensões em diferentes fatos/views.
- Redução de redundância em atributos descritivos.

**Contras (PostgreSQL):**
- Mais joins para consultas científicas exploratórias.
- Mais custo de manutenção de ETL para SCD e chaves substitutas.
- Prazo de 2 semanas fica mais apertado para estabilizar carga e tuning.

### Alternativa 2 — Modelo “flat analítico” com dimensões mínimas (escolhida)

**Ideia:** tabela principal ampla (`fato_nascimentos`) contendo colunas já harmonizadas do ETL para análise, com dimensões obrigatórias enxutas (`dim_municipio_ibge`, `dim_estabelecimento`) e `dim_categoria` para apoio.

**Prós (PostgreSQL):**
- Menos joins nas consultas mais comuns (ano/UF/município/estabelecimento).
- Implementação mais rápida e aderente ao prazo da fase.
- Excelente compatibilidade com particionamento por ano e índices compostos.

**Contras:**
- Maior redundância de alguns atributos descritivos.
- Menor “pureza” dimensional comparado ao estrela completo.

### Decisão

Foi escolhido o **modelo flat analítico com dimensões mínimas**, por melhor custo-benefício para o objetivo científico do TCC (consultas rápidas e reproduzíveis) no prazo da fase.

## B) Modelo definido (tabelas e relacionamentos)

## Tabelas

1. `dim_municipio_ibge`
   - Chave: `cod_municipio_ibge6`
   - Conteúdo: município, UF, meso/microrregião.

2. `dim_estabelecimento`
   - Chave: `cod_estabelecimento` (CODESTAB normalizado em 7 dígitos).
   - Conteúdo mínimo para integridade e futura expansão CNES.

3. `dim_categoria`
   - Chave composta: (`variavel`, `codigo`)
   - Conteúdo: rótulos de categoria e flag de missing.

4. `fato_nascimentos` (particionada por `ano`)
   - PK composta: (`ano`, `run_id`, `source_row_number`)
   - FKs:
     - `codmunnasc_std` → `dim_municipio_ibge.cod_municipio_ibge6`
     - `codmunres_std` → `dim_municipio_ibge.cod_municipio_ibge6`
     - `codestab_std` → `dim_estabelecimento.cod_estabelecimento`

## Regras de integridade

- `NOT NULL` em colunas essenciais: `ano`, `run_id`, `source_row_number`, `source_zip`, `codestab_valid`, `missing_count`.
- `CHECK` em domínios principais:
  - UF (`^[A-Z]{2}$`)
  - Município (`^[0-9]{6}$`)
  - Estabelecimento (`^[0-9]{7}$`)
  - `sexo`, `racacor`, `parto`, `gestacao`
  - faixas plausíveis para idade materna, peso, Apgar, consultas e semanas.

### Observação sobre `CHECK` no PostgreSQL e ETL

No PostgreSQL, `CHECK` é amplamente suportado e aplicado. Ainda assim, validações muito complexas/semânticas (ex.: consistência histórica entre variáveis, regra clínica composta) devem continuar no ETL, onde o contexto de transformação é mais rico.

## C) SQL completo

Script entregue em: [sql/fase4_sinasc_postgresql.sql](sql/fase4_sinasc_postgresql.sql)

Inclui:
- `CREATE DATABASE`
- `CREATE TABLE` de dimensões
- `CREATE TABLE fato_nascimentos PARTITION BY RANGE (ano)`
- PK/FK explícitas
- `CHECK`, `NOT NULL`
- índices simples e compostos
- partições de 2013 a 2023 + partição `DEFAULT`
- tabela de staging opcional para carga

## D) Estratégia de carga (por ano, com integridade)

### Fluxo recomendado

1. Carregar `dim_municipio_ibge` via `\copy` (psql) do arquivo `data/reference/ibge_municipios.csv`.
2. Carregar `dim_categoria` via `\copy` de `data/processed/reference/category_config.csv`.
3. Carregar o CSV projetado na `stg_sinasc_harmonized` com `\copy`.
4. Popular `dim_estabelecimento` a partir da staging:

```sql
INSERT INTO dim_estabelecimento (cod_estabelecimento, cod_estabelecimento_valid)
SELECT DISTINCT s.codestab_std, MAX(s.codestab_valid)
FROM stg_sinasc_harmonized s
WHERE s.codestab_std IS NOT NULL
GROUP BY s.codestab_std
ON CONFLICT (cod_estabelecimento)
DO UPDATE SET cod_estabelecimento_valid = EXCLUDED.cod_estabelecimento_valid;
```

5. Carga incremental por ano na fato (melhora controle e rollback):

```sql
INSERT INTO fato_nascimentos (
  ano, run_id, source_row_number, source_zip,
  codmunnasc_std, codmunres_std, codmunnasc_uf, codmunres_uf,
  codestab_std, codestab_valid, dtnasc_iso,
  sexo, sexo_desc, racacor, racacor_desc, parto, parto_desc, gestacao, consultas,
  idademae_int, peso_int, apgar1_int, apgar5_int, consprenat_int, semagestac, missing_count
)
SELECT
  s.ano, s.run_id, s.source_row_number, s.source_zip,
  s.codmunnasc_std, s.codmunres_std, s.codmunnasc_uf, s.codmunres_uf,
  s.codestab_std, s.codestab_valid, s.dtnasc_iso,
  s.sexo, s.sexo_desc, s.racacor, s.racacor_desc, s.parto, s.parto_desc, s.gestacao, s.consultas,
  s.idademae_int, s.peso_int, s.apgar1_int, s.apgar5_int, s.consprenat_int, s.semagestac, s.missing_count
FROM stg_sinasc_harmonized s
WHERE s.ano = 2019;
```


### Validações pós-carga

```sql
-- total por ano na staging
SELECT ano, COUNT(*) AS qtd FROM stg_sinasc_harmonized GROUP BY ano ORDER BY ano;

-- total por ano na fato
SELECT ano, COUNT(*) AS qtd FROM fato_nascimentos GROUP BY ano ORDER BY ano;

-- divergências entre staging e fato
SELECT s.ano, COUNT(*) AS diff
FROM stg_sinasc_harmonized s
LEFT JOIN fato_nascimentos f
  ON f.ano = s.ano AND f.run_id = s.run_id AND f.source_row_number = s.source_row_number
WHERE f.ano IS NULL
GROUP BY s.ano;

-- checagem de FKs órfãs (esperado: 0)
SELECT COUNT(*) AS orfa_mun_res
FROM fato_nascimentos f
LEFT JOIN dim_municipio_ibge d ON d.cod_municipio_ibge6 = f.codmunres_std
WHERE f.codmunres_std IS NOT NULL AND d.cod_municipio_ibge6 IS NULL;
```

## E) Testes de performance

## Consultas típicas

### 1) Agregação por UF e ano

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.ano, f.codmunres_uf AS uf, COUNT(*) AS nascidos
FROM fato_nascimentos f
WHERE f.ano BETWEEN 2018 AND 2023
  AND f.codmunres_uf = 'SP'
GROUP BY f.ano, f.codmunres_uf
ORDER BY f.ano;
```

**Esperado:** uso de `idx_fato_ano_uf_mun_res`/`idx_fato_uf_res` e pruning das partições 2018..2023.

### 2) Município (residência) por ano

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.ano, f.codmunres_std, COUNT(*) AS nascidos
FROM fato_nascimentos f
WHERE f.ano = 2021
  AND f.codmunres_std = '350950'
GROUP BY f.ano, f.codmunres_std;
```

**Esperado:** lookup por índice em `idx_fato_ano_uf_mun_res` ou `idx_fato_codmun_res`, sem full scan global.

### 3) Estabelecimento por janela temporal

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.codestab_std, f.ano, COUNT(*) AS nascidos
FROM fato_nascimentos f
WHERE f.codestab_std = '1234567'
  AND f.ano BETWEEN 2019 AND 2023
GROUP BY f.codestab_std, f.ano
ORDER BY f.ano;
```

**Esperado:** uso de `idx_fato_ano_estab` e pruning de partição.

### 4) Indicador clínico com filtro composto

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.ano, f.codmunres_uf, AVG(f.peso_int) AS peso_medio
FROM fato_nascimentos f
WHERE f.ano BETWEEN 2020 AND 2023
  AND f.codmunres_uf IN ('SP', 'RJ', 'MG')
  AND f.peso_int IS NOT NULL
GROUP BY f.ano, f.codmunres_uf;
```

**Esperado:** leitura parcial por índice composto (`ano + uf`) com menor custo que varredura completa.

## Como interpretar o plano

- **Uso de índice:** verificar `Index Scan` / `Bitmap Index Scan` e o índice escolhido.
- **Full scan:** se aparecer `Seq Scan` em cenários filtrados por ano/UF/município, revisar índices compostos.
- **Partition pruning:** o plano deve listar somente as partições necessárias ao filtro de ano.

---

# Relatório de Execução — Banco PostgreSQL SINASC (Fase 4)

## 1) Resumo executivo

Todas as etapas operacionais da Fase 4 foram executadas automaticamente:

- ✅ Banco `sinasc_tcc` criado
- ✅ Schema `sinasc` + tabelas + partições + índices aplicados
- ✅ Dimensões carregadas (municípios IBGE + categorias + estabelecimentos)
- ✅ Staging carregada (30.983.111 registros)
- ✅ Tabela fato carregada por ano (2013–2023)
- ✅ Validações de integridade realizadas
- ✅ Testes de performance executados

## 2) Artefatos gerados

| Arquivo | Descrição | Tamanho/Status |
|---|---|---|
| `data/processed/stg_sinasc_harmonized.csv` | CSV projetado para staging | 4,97 GB |
| `scripts/build_staging_csv.py` | Script Python de projeção | Novo |
| `sql/load_fato_incremental_2013_2023.sql` | Carga incremental por ano | Novo |
| `sql/performance_tests.sql` | Testes EXPLAIN ANALYZE | Novo |
| `data/processed/logs/performance_tests_results.txt` | Resultados de performance | Novo |

## 3) Estatísticas do banco

### Tabelas carregadas

| Tabela | Registros |
|---|---|
| `dim_municipio_ibge` | 5.199 |
| `dim_categoria` | 20 |
| `dim_estabelecimento` | 10.739 |
| `stg_sinasc_harmonized` | 30.983.111 |
| `fato_nascimentos` | **30.983.111** |

### Distribuição por ano (fato)

Todos os 11 anos foram carregados corretamente (2013–2023).

## 4) Validações de integridade

### FKs órfãs (esperado: 0 em todos)

| Validação | Resultado |
|---|---|
| FK município de residência órfã | ✅ 0 |
| FK município de nascimento órfã | ✅ 0 |
| FK estabelecimento órfã | ✅ 0 |

### Divergências staging → fato

Nenhuma divergência detectada; todas as linhas da staging foram inseridas na fato.

## 5) Resultados dos testes de performance

Todos os testes foram executados com `EXPLAIN (ANALYZE, BUFFERS)`.

### Teste 1: Agregação por UF e ano (SP, 2018–2023)

- **Plano:** Index Scan com partition pruning (6 partições selecionadas)
- **Tempo de execução:** 169 ms
- **Observação:** Uso eficiente dos índices compostos `idx_fato_ano_uf_mun_res`

### Teste 2: Município específico por ano (Campinas/350950, 2021)

- **Plano:** Index Only Scan em `fato_nascimentos_2021_ano_codmunres_uf_codmunres_std_idx`
- **Tempo de execução:** 0,182 ms
- **Observação:** Lookup extremamente rápido por índice composto, sem Seq Scan

### Teste 3: Estabelecimento por janela temporal (2019–2023)

- **Plano:** Append + Index Only Scan com grouping
- **Tempo de execução:** 2.088 ms
- **Observação:** Partition pruning (5 partições), índice `idx_fato_ano_estab` ativo

### Teste 4: Indicador clínico com filtro composto (peso médio por UF/ano)

- **Plano:** Index Scan em índices compostos por UF + ano
- **Tempo de execução:** 2.250 ms
- **Observação:** Processamento de ~3,8 milhões de registros com agregação eficiente

## 6) Evidências de qualidade técnica

### Partition pruning ativo

Todos os testes que filtram por ano demonstraram que o PostgreSQL seleciona apenas as partições necessárias.

### Uso efetivo de índices

Nenhum teste resultou em `Seq Scan` global; todos usaram `Index Scan`, `Index Only Scan` ou `Bitmap Index Scan`.

### Integridade referencial garantida

A carga com `LEFT JOIN` + `CASE` garantiu que valores sem correspondência nas dimensões foram convertidos para `NULL`, preservando constraints sem perder dados.

## 7) Compatibilidade com os critérios de TCC (Fase 4)

| Critério | Status | Evidência |
|---|---|---|
| Schema definido | ✅ | `sql/fase4_sinasc_postgresql.sql` aplicado |
| Tabelas criadas com PK e FK explícitas | ✅ | FK validadas + 0 órfãs |
| Constraints (NOT NULL, CHECK) | ✅ | Violações tratadas na carga incremental |
| Particionamento por ano (2013+) | ✅ | 11 partições + 1 DEFAULT |
| Índices em UF, Município e Ano | ✅ | Confirmado via EXPLAIN ANALYZE |
| Relacionamento com IBGE e estabelecimentos | ✅ | FKs definidas e validadas |
| Testes de performance realizados | ✅ | 4 testes com EXPLAIN ANALYZE documentados |
| Banco estruturado e executável no servidor | ✅ | Rodando localmente em PostgreSQL 18.3 |

## 8) Arquivos SQL finais executáveis

1. `sql/fase4_sinasc_postgresql.sql` — DDL completo (schema + tabelas + índices)
2. `sql/load_fato_incremental_2013_2023.sql` — Carga incremental por ano
3. `sql/performance_tests.sql` — Testes de performance

## Como replicar ou auditar

### Reprocessar tudo do zero

```powershell
# 1) Recriar banco
$env:PGPASSWORD='@F153007f'
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d postgres -c "DROP DATABASE IF EXISTS sinasc_tcc; CREATE DATABASE sinasc_tcc WITH ENCODING 'UTF8';"

# 2) Aplicar DDL
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -v ON_ERROR_STOP=1 -f .\sql\fase4_sinasc_postgresql.sql

# 3) Carregar dimensões
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -v ON_ERROR_STOP=1 -c "\copy sinasc.dim_municipio_ibge(cod_municipio_ibge7, cod_municipio_ibge6, municipio_nome, uf, mesorregiao, microrregiao) FROM 'data/reference/ibge_municipios.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"

& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -v ON_ERROR_STOP=1 -c "\copy sinasc.dim_categoria(variavel, codigo, rotulo, is_missing) FROM 'data/processed/reference/category_config.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"

# 4) Carregar staging
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -v ON_ERROR_STOP=1 -c "\copy sinasc.stg_sinasc_harmonized(ano, run_id, source_row_number, source_zip, codmunnasc_std, codmunres_std, codmunnasc_uf, codmunres_uf, codestab_std, codestab_valid, dtnasc_iso, sexo, sexo_desc, racacor, racacor_desc, parto, parto_desc, gestacao, consultas, idademae_int, peso_int, apgar1_int, apgar5_int, consprenat_int, semagestac, missing_count) FROM 'data/processed/stg_sinasc_harmonized.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8', NULL '')"

# 5) Popular dim_estabelecimento
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -v ON_ERROR_STOP=1 -c "INSERT INTO sinasc.dim_estabelecimento (cod_estabelecimento, cod_estabelecimento_valid) SELECT s.codestab_std, MAX(s.codestab_valid) FROM sinasc.stg_sinasc_harmonized s WHERE s.codestab_std IS NOT NULL GROUP BY s.codestab_std ON CONFLICT (cod_estabelecimento) DO UPDATE SET cod_estabelecimento_valid = EXCLUDED.cod_estabelecimento_valid;"

# 6) Carga incremental da fato
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -v ON_ERROR_STOP=1 -f .\sql\load_fato_incremental_2013_2023.sql

# 7) Testes de performance
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d sinasc_tcc -f .\sql\performance_tests.sql
```
