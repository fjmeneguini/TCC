import csv
from pathlib import Path


def main() -> None:
    src = Path("data/processed/sinasc_harmonized.csv")
    dst = Path("data/processed/stg_sinasc_harmonized.csv")
    dst.parent.mkdir(parents=True, exist_ok=True)

    mapping = [
        ("YEAR", "ano"),
        ("RUN_ID", "run_id"),
        ("SOURCE_ROW_NUMBER", "source_row_number"),
        ("SOURCE_ZIP", "source_zip"),
        ("CODMUNNASC_STD", "codmunnasc_std"),
        ("CODMUNRES_STD", "codmunres_std"),
        ("CODMUNNASC_UF", "codmunnasc_uf"),
        ("CODMUNRES_UF", "codmunres_uf"),
        ("CODESTAB_STD", "codestab_std"),
        ("CODESTAB_VALID", "codestab_valid"),
        ("DTNASC_ISO", "dtnasc_iso"),
        ("SEXO", "sexo"),
        ("SEXO_DESC", "sexo_desc"),
        ("RACACOR", "racacor"),
        ("RACACOR_DESC", "racacor_desc"),
        ("PARTO", "parto"),
        ("PARTO_DESC", "parto_desc"),
        ("GESTACAO", "gestacao"),
        ("CONSULTAS", "consultas"),
        ("IDADEMAE_INT", "idademae_int"),
        ("PESO_INT", "peso_int"),
        ("APGAR1_INT", "apgar1_int"),
        ("APGAR5_INT", "apgar5_int"),
        ("CONSPRENAT_INT", "consprenat_int"),
        ("SEMAGESTAC", "semagestac"),
        ("MISSING_COUNT", "missing_count"),
    ]

    with src.open("r", encoding="utf-8", newline="") as fin, dst.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=[dest for _, dest in mapping])
        writer.writeheader()

        rows = 0
        for row in reader:
            out = {dest: (row.get(orig, "") or "") for orig, dest in mapping}
            writer.writerow(out)
            rows += 1

    print(f"OK - arquivo gerado: {dst}")
    print(f"OK - linhas escritas: {rows}")


if __name__ == "__main__":
    main()
