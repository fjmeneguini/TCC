SET search_path TO sinasc, public;

\timing on

\echo '=== Teste 1: Agregacao por UF e ano (SP, 2018-2023) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.ano, f.codmunres_uf AS uf, COUNT(*) AS nascidos
FROM fato_nascimentos f
WHERE f.ano BETWEEN 2018 AND 2023
  AND f.codmunres_uf = 'SP'
GROUP BY f.ano, f.codmunres_uf
ORDER BY f.ano;

\echo '=== Teste 2: Municipio especifico por ano (350950, 2021) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.ano, f.codmunres_std, COUNT(*) AS nascidos
FROM fato_nascimentos f
WHERE f.ano = 2021
  AND f.codmunres_std = '350950'
GROUP BY f.ano, f.codmunres_std;

\echo '=== Teste 3: Estabelecimento por janela temporal ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.codestab_std, f.ano, COUNT(*) AS nascidos
FROM fato_nascimentos f
WHERE f.ano BETWEEN 2019 AND 2023
  AND f.codestab_std IS NOT NULL
GROUP BY f.codestab_std, f.ano
ORDER BY f.ano, nascidos DESC
LIMIT 50;

\echo '=== Teste 4: Indicador clinico com filtro composto (peso medio por UF/ano) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.ano, f.codmunres_uf, AVG(f.peso_int) AS peso_medio, COUNT(*) AS qtd
FROM fato_nascimentos f
WHERE f.ano BETWEEN 2020 AND 2023
  AND f.codmunres_uf IN ('SP', 'RJ', 'MG')
  AND f.peso_int IS NOT NULL
GROUP BY f.ano, f.codmunres_uf
ORDER BY f.ano, f.codmunres_uf;
