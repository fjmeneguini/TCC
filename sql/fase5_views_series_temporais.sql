SET search_path TO sinasc, public;

-- View mensal: indice de prematuridade
-- Regra: semagestac < 37 semanas
CREATE OR REPLACE VIEW sinasc.vw_indice_prematuridade_mensal AS
SELECT
  date_trunc('month', f.dtnasc_iso)::date AS mes_ref,
  EXTRACT(YEAR FROM f.dtnasc_iso)::int AS ano,
  EXTRACT(MONTH FROM f.dtnasc_iso)::int AS mes,
  COUNT(*)::bigint AS total_nascimentos,
  SUM(CASE WHEN f.semagestac IS NOT NULL AND f.semagestac < 37 THEN 1 ELSE 0 END)::bigint AS total_prematuros,
  ROUND(
    (
      SUM(CASE WHEN f.semagestac IS NOT NULL AND f.semagestac < 37 THEN 1 ELSE 0 END)::numeric
      / NULLIF(COUNT(*)::numeric, 0)
    ) * 100.0,
    4
  ) AS indice_prematuridade_pct
FROM sinasc.fato_nascimentos f
WHERE f.dtnasc_iso IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1;

COMMENT ON VIEW sinasc.vw_indice_prematuridade_mensal IS
'Indicador mensal de prematuridade: (nascidos com semagestac < 37) / total de nascimentos * 100';


-- View mensal: indice de baixo peso ao nascer
-- Regra: peso_int < 2500 gramas
CREATE OR REPLACE VIEW sinasc.vw_indice_baixo_peso_mensal AS
SELECT
  date_trunc('month', f.dtnasc_iso)::date AS mes_ref,
  EXTRACT(YEAR FROM f.dtnasc_iso)::int AS ano,
  EXTRACT(MONTH FROM f.dtnasc_iso)::int AS mes,
  COUNT(*)::bigint AS total_nascimentos,
  SUM(CASE WHEN f.peso_int IS NOT NULL AND f.peso_int < 2500 THEN 1 ELSE 0 END)::bigint AS total_baixo_peso,
  ROUND(
    (
      SUM(CASE WHEN f.peso_int IS NOT NULL AND f.peso_int < 2500 THEN 1 ELSE 0 END)::numeric
      / NULLIF(COUNT(*)::numeric, 0)
    ) * 100.0,
    4
  ) AS indice_baixo_peso_pct
FROM sinasc.fato_nascimentos f
WHERE f.dtnasc_iso IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1;

COMMENT ON VIEW sinasc.vw_indice_baixo_peso_mensal IS
'Indicador mensal de baixo peso: (nascidos com peso_int < 2500g) / total de nascimentos * 100';
