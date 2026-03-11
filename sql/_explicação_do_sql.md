# EXPLICAÇÃO COMPLETA DO SQL DA FASE 4

Documentação detalhada de toda a estrutura criada no banco PostgreSQL para o projeto SINASC.

---

## **ESTRUTURA GERAL DO BANCO**

O banco se chama **`sinasc_tcc`** e tem um schema organizado chamado **`sinasc`** onde todas as tabelas ficam.

A arquitetura segue um modelo **"flat analítico com dimensões mínimas"**, que é um meio-termo entre uma tabela gigante única e um modelo estrela completo. Isso facilita consultas científicas rápidas sem muitos JOINs.

---

## **AS 5 TABELAS PRINCIPAIS**

### **1. `dim_municipio_ibge` (Dimensão de Municípios)**

**Para que serve:** Armazena todos os municípios brasileiros com seus códigos IBGE oficiais e informações geográficas.

**Estrutura:**
```sql
- cod_municipio_ibge6 (PK) → código de 6 dígitos (ex: "350950" = Campinas/SP)
- cod_municipio_ibge7       → código de 7 dígitos (alternativo)
- municipio_nome            → nome do município (ex: "Campinas")
- uf                        → sigla do estado (ex: "SP")
- mesorregiao              → mesorregião IBGE
- microrregiao             → microrregião IBGE
```

**Quantos registros tem:** 5.199 municípios

**Constraints importantes:**
- Código de 6 dígitos deve ser numérico: `CHECK (cod_municipio_ibge6 ~ '^[0-9]{6}$')`
- UF deve ter 2 letras maiúsculas: `CHECK (uf ~ '^[A-Z]{2}$')`

**Por que existe:** As tabelas de fato referenciam municípios nos campos "município de nascimento" e "município de residência da mãe". Esta tabela garante que só municípios válidos sejam aceitos e permite enrichment (adicionar nome do município nas consultas).

---

### **2. `dim_estabelecimento` (Dimensão de Estabelecimentos de Saúde)**

**Para que serve:** Armazena códigos dos estabelecimentos de saúde (hospitais, maternidades, etc.) onde os nascimentos ocorreram.

**Estrutura:**
```sql
- cod_estabelecimento (PK)    → código CNES de 7 dígitos
- cod_estabelecimento_valid   → flag se o código é válido (0 ou 1)
```

**Quantos registros tem:** 10.739 estabelecimentos

**Constraints importantes:**
- Código deve ter 7 dígitos numéricos: `CHECK (cod_estabelecimento ~ '^[0-9]{7}$')`
- Flag de validade só pode ser 0 ou 1: `CHECK (cod_estabelecimento_valid IN (0, 1))`

**Por que existe:** Durante o ETL, descobrimos que alguns códigos de estabelecimento nos dados originais eram inválidos ou incompletos. Esta tabela permite rastrear quais estabelecimentos existem e se seus códigos foram validados. No futuro, você pode enrichar com dados do CNES (nome do hospital, endereço, tipo, etc.).

---

### **3. `dim_categoria` (Dimensão de Categorias)**

**Para que serve:** Dicionário de todos os códigos categóricos usados no SINASC (sexo, raça/cor, tipo de parto, tipo de gestação, etc.).

**Estrutura:**
```sql
- variavel (PK)  → nome da variável (ex: "SEXO", "RACACOR", "PARTO")
- codigo (PK)    → código específico (ex: "1", "2", "9")
- rotulo         → descrição legível (ex: "Masculino", "Feminino", "Ignorado")
- is_missing     → flag se o código representa dado ausente (0 ou 1)
```

**Quantos registros tem:** 20 categorias

**Exemplo de dados:**
```
SEXO | 1 | Masculino | 0
SEXO | 2 | Feminino  | 0
SEXO | 9 | Ignorado  | 1
PARTO | 1 | Vaginal   | 0
PARTO | 2 | Cesáreo   | 0
```

**Por que existe:** Facilita consultas legíveis (você faz JOIN e pega a descrição ao invés de mostrar códigos numéricos) e documenta formalmente todos os valores válidos das variáveis categóricas.

---

### **4. `fato_nascimentos` (Tabela Fato Principal - PARTICIONADA)**

**Para que serve:** Armazena **todos os registros de nascimentos** do Brasil de 2013 a 2023. É a tabela central do projeto, onde ficam os dados analíticos.

**Quantos registros tem:** 30.983.111 nascimentos (30,9 milhões!)

#### **Campos de rastreabilidade:**
```sql
- ano                    → ano do nascimento (usado para particionamento)
- run_id                 → identificador da execução do ETL (ex: "20260228T183442Z")
- source_row_number (PK) → número da linha no arquivo original
- source_zip             → nome do arquivo ZIP de origem
```

#### **Campos de localização (com FKs):**
```sql
- codmunnasc_std (FK)    → município de nascimento → dim_municipio_ibge
- codmunres_std (FK)     → município de residência → dim_municipio_ibge
- codmunnasc_uf          → UF de nascimento (redundante, mas facilita queries)
- codmunres_uf           → UF de residência (redundante, mas facilita queries)
- codestab_std (FK)      → estabelecimento de saúde → dim_estabelecimento
- codestab_valid         → flag se o código do estabelecimento é válido
```

#### **Campos clínicos e demográficos:**
```sql
- dtnasc_iso             → data de nascimento (formato ISO: AAAA-MM-DD)
- sexo                   → código do sexo ('1'=M, '2'=F, '9'=Ignorado)
- sexo_desc              → descrição do sexo (ex: "Masculino")
- racacor                → código raça/cor ('1' a '5', '9'=Ignorado)
- racacor_desc           → descrição raça/cor
- parto                  → tipo de parto ('1'=Vaginal, '2'=Cesáreo)
- parto_desc             → descrição do parto
- gestacao               → faixa de semanas de gestação
- consultas              → faixa de consultas pré-natal
- idademae_int           → idade da mãe (inteiro)
- peso_int               → peso ao nascer em gramas (inteiro)
- apgar1_int             → Apgar 1º minuto (0-10)
- apgar5_int             → Apgar 5º minuto (0-10)
- consprenat_int         → número de consultas pré-natal
- semagestac             → semanas de gestação (inteiro)
- missing_count          → contador de campos ausentes neste registro
```

#### **Chave primária composta:**
```sql
PRIMARY KEY (ano, run_id, source_row_number)
```
Isso garante que cada linha do ETL seja única e permite rastrear de onde veio cada registro.

#### **Constraints de integridade:**

**FKs (Chaves Estrangeiras):**
- `codmunnasc_std` deve existir em `dim_municipio_ibge`
- `codmunres_std` deve existir em `dim_municipio_ibge`
- `codestab_std` deve existir em `dim_estabelecimento`

**CHECKs de formato:**
- `ano BETWEEN 2013 AND 2100` (valida recorte temporal)
- `codmunres_std ~ '^[0-9]{6}$'` (município deve ter 6 dígitos)
- `codestab_std ~ '^[0-9]{7}$'` (estabelecimento deve ter 7 dígitos)
- `uf ~ '^[A-Z]{2}$'` (UF deve ter 2 letras)

**CHECKs de domínio categórico:**
- `sexo IN ('0', '1', '2', '9')`
- `racacor IN ('1', '2', '3', '4', '5', '9')`
- `parto IN ('1', '2', '9')`

**CHECKs de faixas plausíveis:**
- `idademae_int BETWEEN 10 AND 70` (idade materna razoável)
- `peso_int BETWEEN 100 AND 9999` (peso em gramas plausível)
- `apgar1_int BETWEEN 0 AND 10` (escala Apgar vai de 0 a 10)
- `apgar5_int BETWEEN 0 AND 10`
- `semagestac BETWEEN 0 AND 60` (gestação plausível)

---

### **PARTICIONAMENTO POR ANO**

A tabela `fato_nascimentos` **não armazena fisicamente os dados em uma única tabela**. Ela é dividida em **12 partições**:

```sql
fato_nascimentos_2013  → nascimentos de 2013
fato_nascimentos_2014  → nascimentos de 2014
fato_nascimentos_2015  → nascimentos de 2015
...
fato_nascimentos_2023  → nascimentos de 2023
fato_nascimentos_pmax  → partição DEFAULT (para anos futuros)
```

**Por que particionar?**

1. **Performance:** Quando você consulta dados de 2021, o PostgreSQL só lê a partição de 2021, ignorando os outros 10 anos (isso se chama **partition pruning**)

2. **Manutenção:** Você pode fazer backup/restore de um ano específico

3. **Escalabilidade:** Se chegar 2024, basta criar uma nova partição

**Exemplo prático:**
```sql
-- Esta consulta só lê a partição 2021 (partition pruning)
SELECT COUNT(*) FROM fato_nascimentos WHERE ano = 2021;

-- Esta consulta lê 3 partições (2021, 2022, 2023)
SELECT COUNT(*) FROM fato_nascimentos WHERE ano BETWEEN 2021 AND 2023;
```

Nos testes de performance o partition pruning está funcionando perfeitamente!

---

### **5. `stg_sinasc_harmonized` (Tabela de Staging)**

**Para que serve:** Área temporária onde os dados do CSV harmonizado (saída do ETL) são carregados antes de serem inseridos na tabela fato.

**Quantos registros tem:** 30.983.111 (igual à fato, mas depois da carga pode ser truncada)

**Por que existe:**

1. **Validação antes da carga final:** Você carrega tudo na staging, valida, e só então move para a fato
2. **Facilita popular a `dim_estabelecimento`:** Você extrai os códigos únicos da staging
3. **Permite carga incremental ano a ano:** Você carrega staging completa, depois vai inserindo na fato ano por ano
4. **Tipo `UNLOGGED`:** Mais rápida porque não grava logs de transação (usada apenas para carga temporária)

**Estrutura:** Mesmos campos da `fato_nascimentos`, mas sem constraints rígidas (aceita dados imperfeitos).

---

## **ÍNDICES ESTRATÉGICOS**

Índices fazem as consultas ficarem rápidas. Foram criados 10 índices na tabela fato:

### **Índices simples (1 coluna):**
```sql
idx_fato_ano           → consultas por ano
idx_fato_uf_res        → consultas por UF de residência
idx_fato_uf_nasc       → consultas por UF de nascimento
idx_fato_codmun_res    → consultas por município de residência
idx_fato_codmun_nasc   → consultas por município de nascimento
idx_fato_codestab      → consultas por estabelecimento
```

### **Índices compostos (múltiplas colunas):**
```sql
idx_fato_ano_uf_mun_res    → (ano, codmunres_uf, codmunres_std)
idx_fato_ano_uf_mun_nasc   → (ano, codmunnasc_uf, codmunnasc_std)
idx_fato_ano_estab         → (ano, codestab_std)
idx_fato_ano_peso          → (ano, peso_int)
```

**Por que índices compostos?**

Consultas típicas filtram por **ano + UF** ou **ano + município**. Com índice composto, o PostgreSQL resolve tudo em uma única leitura de índice, sem precisar varrer a tabela.

**Exemplo prático:**
```sql
SELECT COUNT(*) 
FROM fato_nascimentos 
WHERE ano = 2021 AND codmunres_std = '350950';
```

---

## **FLUXO DE CARGA (COMO OS DADOS ENTRAM NO BANCO)**

### **Passo 1: Carregar dimensões**
```sql
-- Carregar 5.199 municípios IBGE
\copy dim_municipio_ibge FROM 'ibge_municipios.csv' WITH CSV HEADER;

-- Carregar 20 categorias
\copy dim_categoria FROM 'category_config.csv' WITH CSV HEADER;
```

### **Passo 2: Carregar staging**
```sql
-- Carregar 30,9 milhões de nascimentos na staging
\copy stg_sinasc_harmonized FROM 'stg_sinasc_harmonized.csv' WITH CSV HEADER;
```

### **Passo 3: Popular dim_estabelecimento**
```sql
-- Extrai códigos únicos de estabelecimentos da staging
INSERT INTO dim_estabelecimento (cod_estabelecimento, cod_estabelecimento_valid)
SELECT DISTINCT codestab_std, MAX(codestab_valid)
FROM stg_sinasc_harmonized
WHERE codestab_std IS NOT NULL
GROUP BY codestab_std;
```

### **Passo 4: Carga incremental ano a ano**

Este é o passo mais inteligente! Ao invés de inserir 30 milhões de linhas de uma vez, carregamos **ano por ano** usando o script `load_fato_incremental_2013_2023.sql`:

```sql
-- Para cada ano de 2013 a 2023...
FOR y IN 2013..2023 LOOP
    INSERT INTO fato_nascimentos (...)
    SELECT 
        s.*,
        -- Tratamento de FKs: se não existe na dimensão, converte para NULL
        CASE WHEN dm_res.cod_municipio_ibge6 IS NOT NULL 
             THEN s.codmunres_std ELSE NULL END,
        -- Normalização de sexo: se vier 'M' ou 'F', converte para '1' ou '2'
        CASE 
            WHEN s.sexo IN ('0','1','2','9') THEN s.sexo
            WHEN UPPER(s.sexo) IN ('M', 'MASCULINO') THEN '1'
            WHEN UPPER(s.sexo) IN ('F', 'FEMININO') THEN '2'
            ELSE NULL
        END
    FROM stg_sinasc_harmonized s
    LEFT JOIN dim_municipio_ibge dm_res ON dm_res.cod_municipio_ibge6 = s.codmunres_std
    LEFT JOIN dim_estabelecimento de ON de.cod_estabelecimento = s.codestab_std
    WHERE s.ano = y;
END LOOP;
```

**Por que esse LEFT JOIN + CASE?**

Porque durante o ETL descobrimos que alguns códigos de município ou estabelecimento não existem nas tabelas de dimensão. Ao invés de deixar a carga falhar, **convertemos FKs inválidas para NULL**. Assim, não perdemos o registro, mas marcamos que aquele campo tem problema.

**Resultado:** 0 FKs órfãs, 100% dos dados carregados com sucesso!

---

## **VALIDAÇÕES PÓS-CARGA**

Depois de carregar tudo, executamos validações para garantir integridade:

```sql
-- 1) Conferir se staging e fato têm mesma quantidade
SELECT COUNT(*) FROM stg_sinasc_harmonized;  -- 30.983.111
SELECT COUNT(*) FROM fato_nascimentos;       -- 30.983.111 ✅

-- 2) Conferir FKs órfãs (esperado: 0)
SELECT COUNT(*) FROM fato_nascimentos f
LEFT JOIN dim_municipio_ibge d ON d.cod_municipio_ibge6 = f.codmunres_std
WHERE f.codmunres_std IS NOT NULL AND d.cod_municipio_ibge6 IS NULL;
-- Resultado: 0 ✅

-- 3) Distribuição por ano
SELECT ano, COUNT(*) FROM fato_nascimentos GROUP BY ano ORDER BY ano;
-- Resultado: todos os 11 anos carregados ✅
```

---

## **TESTES DE PERFORMANCE**

Foram executados 4 testes com `EXPLAIN ANALYZE` para confirmar que índices e particionamento funcionam:

**Teste 1:** Agregação por UF+ano → **169ms** (Index Scan com partition pruning) ✅  
**Teste 2:** Lookup município específico → **0,182ms** (Index Only Scan) ✅  
**Teste 3:** Estabelecimento janela temporal → **2.088ms** (5 partições, índice composto) ✅  
**Teste 4:** Indicador clínico com filtros → **2.250ms** (3,8M registros processados) ✅

Todos os testes confirmaram que **não há Seq Scan** (varredura completa da tabela), ou seja, PostgreSQL está usando índices eficientemente!

---

## **RESUMO EXECUTIVO**

| Tabela | Tipo | Registros | Função |
|--------|------|-----------|--------|
| `dim_municipio_ibge` | Dimensão | 5.199 | Municípios brasileiros com códigos IBGE |
| `dim_estabelecimento` | Dimensão | 10.739 | Estabelecimentos de saúde (hospitais) |
| `dim_categoria` | Dimensão | 20 | Dicionário de códigos categóricos |
| `fato_nascimentos` | Fato (particionada) | 30.983.111 | **Tabela principal com todos os nascimentos** |
| `stg_sinasc_harmonized` | Staging | 30.983.111 | Área temporária para carga |

**Total de partições:** 12 (2013–2023 + DEFAULT)  
**Total de índices:** 10 (6 simples + 4 compostos)  
**Total de constraints:** 23 CHECKs + 3 FKs + PKs  
**Integridade:** 100% validada (0 FKs órfãs)

---

## **ARQUIVOS SQL DO PROJETO**

1. **`sql/fase4_sinasc_postgresql.sql`** — DDL completo (criação do schema, tabelas, partições, índices)
2. **`sql/load_fato_incremental_2013_2023.sql`** — Script de carga incremental por ano com tratamento de FKs
3. **`sql/performance_tests.sql`** — Testes EXPLAIN ANALYZE para validar otimizações

