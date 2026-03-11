-- Fase 4 - Modelagem do Banco (SINASC 2013+)
-- PostgreSQL 14+ 

-- ==========================================
-- 0) CRIAÇÃO DO BANCO 
-- ==========================================
-- CREATE DATABASE sinasc_tcc WITH ENCODING 'UTF8';
-- \c sinasc_tcc

CREATE SCHEMA IF NOT EXISTS sinasc;
SET search_path TO sinasc, public;

-- =========================
-- 1) TABELAS DE DIMENSÃO
-- =========================

CREATE TABLE IF NOT EXISTS dim_municipio_ibge (
  cod_municipio_ibge6 VARCHAR(6) PRIMARY KEY,
  cod_municipio_ibge7 VARCHAR(7),
  municipio_nome VARCHAR(120) NOT NULL,
  uf VARCHAR(2) NOT NULL,
  mesorregiao VARCHAR(120),
  microrregiao VARCHAR(120),
  CONSTRAINT chk_dim_municipio_cod6 CHECK (cod_municipio_ibge6 ~ '^[0-9]{6}$'),
  CONSTRAINT chk_dim_municipio_cod7 CHECK (cod_municipio_ibge7 IS NULL OR cod_municipio_ibge7 ~ '^[0-9]{7}$'),
  CONSTRAINT chk_dim_municipio_uf CHECK (uf ~ '^[A-Z]{2}$')
);

CREATE INDEX IF NOT EXISTS idx_dim_municipio_uf
  ON dim_municipio_ibge (uf);

CREATE TABLE IF NOT EXISTS dim_estabelecimento (
  cod_estabelecimento VARCHAR(7) PRIMARY KEY,
  cod_estabelecimento_valid SMALLINT NOT NULL DEFAULT 1,
  CONSTRAINT chk_dim_estab_codigo CHECK (cod_estabelecimento ~ '^[0-9]{7}$'),
  CONSTRAINT chk_dim_estab_valid CHECK (cod_estabelecimento_valid IN (0, 1))
);

CREATE TABLE IF NOT EXISTS dim_categoria (
  variavel VARCHAR(40) NOT NULL,
  codigo VARCHAR(10) NOT NULL,
  rotulo VARCHAR(120) NOT NULL,
  is_missing SMALLINT NOT NULL DEFAULT 0,
  PRIMARY KEY (variavel, codigo),
  CONSTRAINT chk_dim_categoria_missing CHECK (is_missing IN (0, 1))
);

-- =========================
-- 2) TABELA FATO (PARTICIONADA)
-- =========================

CREATE TABLE IF NOT EXISTS fato_nascimentos (
  ano SMALLINT NOT NULL,
  run_id VARCHAR(16) NOT NULL,
  source_row_number INTEGER NOT NULL,
  source_zip VARCHAR(260) NOT NULL,

  codmunnasc_std VARCHAR(6),
  codmunres_std VARCHAR(6),
  codmunnasc_uf VARCHAR(2),
  codmunres_uf VARCHAR(2),
  codestab_std VARCHAR(7),
  codestab_valid SMALLINT NOT NULL DEFAULT 0,

  dtnasc_iso DATE,

  sexo VARCHAR(1),
  sexo_desc VARCHAR(20),
  racacor VARCHAR(1),
  racacor_desc VARCHAR(20),
  parto VARCHAR(1),
  parto_desc VARCHAR(20),
  gestacao VARCHAR(1),
  consultas VARCHAR(2),

  idademae_int SMALLINT,
  peso_int INTEGER,
  apgar1_int SMALLINT,
  apgar5_int SMALLINT,
  consprenat_int SMALLINT,
  semagestac SMALLINT,
  missing_count SMALLINT NOT NULL DEFAULT 0,

  CONSTRAINT pk_fato_nascimentos PRIMARY KEY (ano, run_id, source_row_number),

  CONSTRAINT fk_fato_mun_nasc
    FOREIGN KEY (codmunnasc_std)
    REFERENCES dim_municipio_ibge (cod_municipio_ibge6),
  CONSTRAINT fk_fato_mun_res
    FOREIGN KEY (codmunres_std)
    REFERENCES dim_municipio_ibge (cod_municipio_ibge6),
  CONSTRAINT fk_fato_estab
    FOREIGN KEY (codestab_std)
    REFERENCES dim_estabelecimento (cod_estabelecimento),

  CONSTRAINT chk_fato_ano CHECK (ano >= 2013 AND ano <= 2100),
  CONSTRAINT chk_fato_source_row CHECK (source_row_number > 0),
  CONSTRAINT chk_fato_codmunnasc CHECK (codmunnasc_std IS NULL OR codmunnasc_std ~ '^[0-9]{6}$'),
  CONSTRAINT chk_fato_codmunres CHECK (codmunres_std IS NULL OR codmunres_std ~ '^[0-9]{6}$'),
  CONSTRAINT chk_fato_uf_nasc CHECK (codmunnasc_uf IS NULL OR codmunnasc_uf ~ '^[A-Z]{2}$'),
  CONSTRAINT chk_fato_uf_res CHECK (codmunres_uf IS NULL OR codmunres_uf ~ '^[A-Z]{2}$'),
  CONSTRAINT chk_fato_codestab CHECK (codestab_std IS NULL OR codestab_std ~ '^[0-9]{7}$'),
  CONSTRAINT chk_fato_codestab_valid CHECK (codestab_valid IN (0, 1)),

  CONSTRAINT chk_fato_sexo CHECK (sexo IS NULL OR sexo IN ('0', '1', '2', '9')),
  CONSTRAINT chk_fato_racacor CHECK (racacor IS NULL OR racacor IN ('1', '2', '3', '4', '5', '9')),
  CONSTRAINT chk_fato_parto CHECK (parto IS NULL OR parto IN ('1', '2', '9')),
  CONSTRAINT chk_fato_gestacao CHECK (gestacao IS NULL OR gestacao IN ('1', '2', '3', '4', '5', '6', '9')),

  CONSTRAINT chk_fato_idademae CHECK (idademae_int IS NULL OR idademae_int BETWEEN 10 AND 70),
  CONSTRAINT chk_fato_peso CHECK (peso_int IS NULL OR peso_int BETWEEN 100 AND 9999),
  CONSTRAINT chk_fato_apgar1 CHECK (apgar1_int IS NULL OR apgar1_int BETWEEN 0 AND 10),
  CONSTRAINT chk_fato_apgar5 CHECK (apgar5_int IS NULL OR apgar5_int BETWEEN 0 AND 10),
  CONSTRAINT chk_fato_consprenat CHECK (consprenat_int IS NULL OR consprenat_int BETWEEN 0 AND 99),
  CONSTRAINT chk_fato_semagestac CHECK (semagestac IS NULL OR semagestac BETWEEN 0 AND 60),
  CONSTRAINT chk_fato_missing_count CHECK (missing_count >= 0)
) PARTITION BY RANGE (ano);

-- Partições anuais 2013..2023
CREATE TABLE IF NOT EXISTS fato_nascimentos_2013 PARTITION OF fato_nascimentos FOR VALUES FROM (2013) TO (2014);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2014 PARTITION OF fato_nascimentos FOR VALUES FROM (2014) TO (2015);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2015 PARTITION OF fato_nascimentos FOR VALUES FROM (2015) TO (2016);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2016 PARTITION OF fato_nascimentos FOR VALUES FROM (2016) TO (2017);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2017 PARTITION OF fato_nascimentos FOR VALUES FROM (2017) TO (2018);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2018 PARTITION OF fato_nascimentos FOR VALUES FROM (2018) TO (2019);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2019 PARTITION OF fato_nascimentos FOR VALUES FROM (2019) TO (2020);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2020 PARTITION OF fato_nascimentos FOR VALUES FROM (2020) TO (2021);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2021 PARTITION OF fato_nascimentos FOR VALUES FROM (2021) TO (2022);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2022 PARTITION OF fato_nascimentos FOR VALUES FROM (2022) TO (2023);
CREATE TABLE IF NOT EXISTS fato_nascimentos_2023 PARTITION OF fato_nascimentos FOR VALUES FROM (2023) TO (2024);
CREATE TABLE IF NOT EXISTS fato_nascimentos_pmax PARTITION OF fato_nascimentos DEFAULT;

-- Índices obrigatórios
CREATE INDEX IF NOT EXISTS idx_fato_ano ON fato_nascimentos (ano);
CREATE INDEX IF NOT EXISTS idx_fato_uf_res ON fato_nascimentos (codmunres_uf);
CREATE INDEX IF NOT EXISTS idx_fato_uf_nasc ON fato_nascimentos (codmunnasc_uf);
CREATE INDEX IF NOT EXISTS idx_fato_codmun_res ON fato_nascimentos (codmunres_std);
CREATE INDEX IF NOT EXISTS idx_fato_codmun_nasc ON fato_nascimentos (codmunnasc_std);
CREATE INDEX IF NOT EXISTS idx_fato_codestab ON fato_nascimentos (codestab_std);

CREATE INDEX IF NOT EXISTS idx_fato_ano_uf_mun_res ON fato_nascimentos (ano, codmunres_uf, codmunres_std);
CREATE INDEX IF NOT EXISTS idx_fato_ano_uf_mun_nasc ON fato_nascimentos (ano, codmunnasc_uf, codmunnasc_std);
CREATE INDEX IF NOT EXISTS idx_fato_ano_estab ON fato_nascimentos (ano, codestab_std);
CREATE INDEX IF NOT EXISTS idx_fato_ano_peso ON fato_nascimentos (ano, peso_int);

-- =========================
-- 3) STAGING PARA CARGA 
-- =========================

CREATE UNLOGGED TABLE IF NOT EXISTS stg_sinasc_harmonized (
  ano SMALLINT NOT NULL,
  run_id VARCHAR(16) NOT NULL,
  source_row_number INTEGER NOT NULL,
  source_zip VARCHAR(260) NOT NULL,

  codmunnasc_std VARCHAR(6),
  codmunres_std VARCHAR(6),
  codmunnasc_uf VARCHAR(2),
  codmunres_uf VARCHAR(2),
  codestab_std VARCHAR(7),
  codestab_valid SMALLINT NOT NULL,

  dtnasc_iso DATE,

  sexo VARCHAR(1),
  sexo_desc VARCHAR(20),
  racacor VARCHAR(1),
  racacor_desc VARCHAR(20),
  parto VARCHAR(1),
  parto_desc VARCHAR(20),
  gestacao VARCHAR(1),
  consultas VARCHAR(2),

  idademae_int SMALLINT,
  peso_int INTEGER,
  apgar1_int SMALLINT,
  apgar5_int SMALLINT,
  consprenat_int SMALLINT,
  semagestac SMALLINT,
  missing_count SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_ano ON stg_sinasc_harmonized (ano);
CREATE INDEX IF NOT EXISTS idx_stg_mun_res ON stg_sinasc_harmonized (codmunres_std);
CREATE INDEX IF NOT EXISTS idx_stg_estab ON stg_sinasc_harmonized (codestab_std);