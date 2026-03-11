SET search_path TO sinasc, public;

DO $$
DECLARE
    y int;
BEGIN
    FOR y IN 2013..2023 LOOP
        EXECUTE format(
            $f$
            INSERT INTO sinasc.fato_nascimentos (
              ano, run_id, source_row_number, source_zip,
              codmunnasc_std, codmunres_std, codmunnasc_uf, codmunres_uf,
              codestab_std, codestab_valid, dtnasc_iso,
              sexo, sexo_desc, racacor, racacor_desc, parto, parto_desc, gestacao, consultas,
              idademae_int, peso_int, apgar1_int, apgar5_int, consprenat_int, semagestac, missing_count
            )
            SELECT
              s.ano, s.run_id, s.source_row_number, s.source_zip,
              CASE WHEN dm_nasc.cod_municipio_ibge6 IS NOT NULL THEN s.codmunnasc_std ELSE NULL END,
              CASE WHEN dm_res.cod_municipio_ibge6 IS NOT NULL THEN s.codmunres_std ELSE NULL END,
              s.codmunnasc_uf, s.codmunres_uf,
              CASE WHEN de.cod_estabelecimento IS NOT NULL THEN s.codestab_std ELSE NULL END,
              s.codestab_valid, s.dtnasc_iso,
              CASE
                WHEN s.sexo IN ('0', '1', '2', '9') THEN s.sexo
                WHEN UPPER(COALESCE(s.sexo, '')) IN ('M', 'MASCULINO') THEN '1'
                WHEN UPPER(COALESCE(s.sexo, '')) IN ('F', 'FEMININO') THEN '2'
                ELSE NULL
              END,
              s.sexo_desc,
              CASE WHEN s.racacor IN ('1', '2', '3', '4', '5', '9') THEN s.racacor ELSE NULL END,
              s.racacor_desc,
              CASE WHEN s.parto IN ('1', '2', '9') THEN s.parto ELSE NULL END,
              s.parto_desc,
              CASE WHEN s.gestacao IN ('1', '2', '3', '4', '5', '6', '9') THEN s.gestacao ELSE NULL END,
              s.consultas,
              s.idademae_int, s.peso_int, s.apgar1_int, s.apgar5_int, s.consprenat_int, s.semagestac, s.missing_count
            FROM sinasc.stg_sinasc_harmonized s
            LEFT JOIN sinasc.dim_municipio_ibge dm_nasc
              ON dm_nasc.cod_municipio_ibge6 = s.codmunnasc_std
            LEFT JOIN sinasc.dim_municipio_ibge dm_res
              ON dm_res.cod_municipio_ibge6 = s.codmunres_std
            LEFT JOIN sinasc.dim_estabelecimento de
              ON de.cod_estabelecimento = s.codestab_std
            WHERE s.ano = %s
            ON CONFLICT (ano, run_id, source_row_number) DO NOTHING
            $f$,
            y
        );

        RAISE NOTICE 'Ano % carregado', y;
    END LOOP;
END $$;
