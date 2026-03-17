import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


SQL_PREMATURIDADE = """
SELECT mes_ref, ano, mes, total_nascimentos, total_prematuros, indice_prematuridade_pct
FROM sinasc.vw_indice_prematuridade_mensal
ORDER BY mes_ref
"""


SQL_BAIXO_PESO = """
SELECT mes_ref, ano, mes, total_nascimentos, total_baixo_peso, indice_baixo_peso_pct
FROM sinasc.vw_indice_baixo_peso_mensal
ORDER BY mes_ref
"""


def build_connection_url(host: str, port: int, database: str, user: str, password: str) -> str:
    return URL.create(
        drivername="postgresql+psycopg",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta views de series temporais do PostgreSQL para CSV.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="sinasc_tcc")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", required=True)
    parser.add_argument("--output-dir", default="data/processed/series_temporais")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(
        build_connection_url(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
        ),
        pool_pre_ping=True,
    )

    df_prem = pd.read_sql(SQL_PREMATURIDADE, engine, parse_dates=["mes_ref"])
    df_baixo = pd.read_sql(SQL_BAIXO_PESO, engine, parse_dates=["mes_ref"])

    out_prem = output_dir / "vw_indice_prematuridade_mensal.csv"
    out_baixo = output_dir / "vw_indice_baixo_peso_mensal.csv"

    df_prem.to_csv(out_prem, index=False, encoding="utf-8")
    df_baixo.to_csv(out_baixo, index=False, encoding="utf-8")

    print(f"[OK] Prematuridade exportada: {out_prem}")
    print(f"[OK] Linhas: {len(df_prem)}")
    print(f"[OK] Baixo peso exportado: {out_baixo}")
    print(f"[OK] Linhas: {len(df_baixo)}")


if __name__ == "__main__":
    main()