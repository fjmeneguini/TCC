import argparse
import csv
import datetime as dt
import gzip
import json
from pathlib import Path
import urllib.request
import zipfile


ETL_VERSION = "1.0.0"
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
MIN_YEAR = 2013

MISSING_MARKERS = {
    "",
    "NA",
    "N/A",
    "NULL",
    "NONE",
    "IGN",
    "IGNORADO",
    "SEM INFORMACAO",
    "SEM INFORMAÇÃO",
    ".",
}

CATEGORY_CONFIG = {
    "SEXO": {
        "0": "Ignorado",
        "1": "Masculino",
        "2": "Feminino",
        "9": "Ignorado",
    },
    "RACACOR": {
        "1": "Branca",
        "2": "Preta",
        "3": "Amarela",
        "4": "Parda",
        "5": "Indígena",
        "9": "Ignorado",
    },
    "PARTO": {
        "1": "Vaginal",
        "2": "Cesáreo",
        "9": "Ignorado",
    },
    "GESTACAO": {
        "1": "Menos de 22 semanas",
        "2": "22 a 27 semanas",
        "3": "28 a 31 semanas",
        "4": "32 a 36 semanas",
        "5": "37 a 41 semanas",
        "6": "42 semanas e mais",
        "9": "Ignorado",
    },
}

DERIVED_COLUMNS = [
    "TRANSFORM_VERSION",
    "RUN_ID",
    "YEAR",
    "SOURCE_ZIP",
    "SOURCE_ROW_NUMBER",
    "CODMUNNASC_STD",
    "CODMUNRES_STD",
    "CODMUNNASC_UF",
    "CODMUNRES_UF",
    "CODMUNNASC_NOME",
    "CODMUNRES_NOME",
    "CODESTAB_STD",
    "CODESTAB_VALID",
    "DTNASC_ISO",
    "IDADEMAE_INT",
    "PESO_INT",
    "APGAR1_INT",
    "APGAR5_INT",
    "CONSPRENAT_INT",
    "SEXO_DESC",
    "RACACOR_DESC",
    "PARTO_DESC",
    "MISSING_COUNT",
]


def now_utc_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def clean_value(value: str) -> str:
    if value is None:
        return ""

    cleaned = str(value).strip()
    if cleaned.upper() in MISSING_MARKERS:
        return ""
    return cleaned


def normalize_colname(column_name: str) -> str:
    normalized = str(column_name).strip().upper()
    if normalized.startswith("\ufeff"):
        normalized = normalized.replace("\ufeff", "", 1)
    return normalized


def digits_only(value: str) -> str:
    return "".join(char for char in value if char.isdigit())


def normalize_municipio_code(value: str) -> str:
    cleaned = digits_only(clean_value(value))
    if not cleaned:
        return ""
    if len(cleaned) >= 7:
        return cleaned[:6]
    if len(cleaned) == 6:
        return cleaned
    return ""


def normalize_codestab(value: str) -> tuple[str, str]:
    cleaned = digits_only(clean_value(value))
    if not cleaned:
        return "", "0"

    cleaned = cleaned.zfill(7)
    is_valid = len(cleaned) == 7 and cleaned not in {"0000000", "9999999"}
    return cleaned if is_valid else "", "1" if is_valid else "0"


def parse_int(value: str, min_value: int | None = None, max_value: int | None = None) -> str:
    cleaned = digits_only(clean_value(value))
    if not cleaned:
        return ""

    try:
        number = int(cleaned)
    except ValueError:
        return ""

    if min_value is not None and number < min_value:
        return ""
    if max_value is not None and number > max_value:
        return ""
    return str(number)


def parse_date_ddmmyyyy(value: str) -> str:
    cleaned = digits_only(clean_value(value))
    if len(cleaned) != 8:
        return ""

    try:
        parsed = dt.datetime.strptime(cleaned, "%d%m%Y").date()
    except ValueError:
        return ""
    return parsed.isoformat()


def category_label(variable: str, code: str) -> str:
    normalized_code = clean_value(code)
    if not normalized_code:
        return ""
    return CATEGORY_CONFIG.get(variable, {}).get(normalized_code, "Não mapeado")


def list_input_files(raw_dir: Path, start_year: int | None, end_year: int | None) -> list[tuple[int, Path]]:
    files: list[tuple[int, Path]] = []
    for year_dir in sorted(raw_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        if not year_dir.name.isdigit():
            continue

        year = int(year_dir.name)
        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue

        matches = sorted(year_dir.glob("SINASC_*_csv.zip"))
        for item in matches:
            files.append((year, item))
    return files


def get_inner_csv_name(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path, "r") as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"Arquivo sem CSV interno: {zip_path}")
        return csv_names[0]


def read_first_line_from_zip(zip_path: Path) -> str:
    inner_name = get_inner_csv_name(zip_path)
    with zipfile.ZipFile(zip_path, "r") as archive:
        with archive.open(inner_name, "r") as buffer:
            first_line = buffer.readline()
    return first_line.decode("latin-1", errors="replace")


def detect_delimiter(zip_path: Path) -> str:
    first_line = read_first_line_from_zip(zip_path)
    semicolon_count = first_line.count(";")
    comma_count = first_line.count(",")
    return ";" if semicolon_count >= comma_count else ","


def read_header_from_zip(zip_path: Path) -> list[str]:
    inner_name = get_inner_csv_name(zip_path)
    delimiter = detect_delimiter(zip_path)
    with zipfile.ZipFile(zip_path, "r") as archive:
        with archive.open(inner_name, "r") as buffer:
            text_stream = (line.decode("latin-1", errors="replace") for line in buffer)
            reader = csv.reader(text_stream, delimiter=delimiter, quotechar='"')
            header = next(reader)
    normalized = [normalize_colname(column) for column in header]
    return [column for column in normalized if column]


def build_union_columns(files: list[tuple[int, Path]]) -> list[str]:
    union: list[str] = []
    seen: set[str] = set()
    for _, zip_path in files:
        for column in read_header_from_zip(zip_path):
            if not column:
                continue
            if column not in seen:
                seen.add(column)
                union.append(column)
    return union


def fetch_ibge_municipios(reference_dir: Path) -> dict[str, dict[str, str]]:
    reference_dir.mkdir(parents=True, exist_ok=True)
    cache_path = reference_dir / "ibge_municipios.csv"

    rows: list[dict[str, str]] = []
    fetch_error = None

    try:
        with urllib.request.urlopen(IBGE_MUNICIPIOS_URL, timeout=120) as response:
            raw_bytes = response.read()

        if raw_bytes[:2] == b"\x1f\x8b":
            raw_bytes = gzip.decompress(raw_bytes)

        payload = json.loads(raw_bytes.decode("utf-8"))

        for item in payload:
            ibge7 = str(item.get("id", ""))
            ibge6 = ibge7[:6] if len(ibge7) >= 6 else ""

            uf = (
                item.get("microrregiao", {})
                .get("mesorregiao", {})
                .get("UF", {})
                .get("sigla", "")
            )

            rows.append(
                {
                    "ibge7": ibge7,
                    "ibge6": ibge6,
                    "municipio": item.get("nome", ""),
                    "uf": uf,
                    "mesorregiao": item.get("microrregiao", {}).get("mesorregiao", {}).get("nome", ""),
                    "microrregiao": item.get("microrregiao", {}).get("nome", ""),
                }
            )
    except Exception as exc:
        fetch_error = f"{exc.__class__.__name__}: {exc}"

    if rows:
        with cache_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(
                output,
                fieldnames=["ibge7", "ibge6", "municipio", "uf", "mesorregiao", "microrregiao"],
            )
            writer.writeheader()
            writer.writerows(rows)
    elif cache_path.exists():
        with cache_path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            rows = [dict(row) for row in reader]
    else:
        raise RuntimeError(
            "Não foi possível carregar tabela IBGE (API indisponível e sem cache local)."
            + (f" Erro: {fetch_error}" if fetch_error else "")
        )

    mapping: dict[str, dict[str, str]] = {}
    for row in rows:
        ibge6 = row.get("ibge6", "")
        if not ibge6:
            continue
        mapping[ibge6] = {
            "municipio": row.get("municipio", ""),
            "uf": row.get("uf", ""),
        }
    return mapping


def write_category_config(processed_dir: Path) -> Path:
    out_path = processed_dir / "reference" / "category_config.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=["variable", "code", "label", "is_missing"])
        writer.writeheader()
        for variable, mapping in CATEGORY_CONFIG.items():
            for code, label in mapping.items():
                writer.writerow(
                    {
                        "variable": variable,
                        "code": code,
                        "label": label,
                        "is_missing": "1" if code in {"0", "9"} else "0",
                    }
                )
    return out_path


def process_zip_file(
    year: int,
    zip_path: Path,
    union_columns: list[str],
    writer: csv.DictWriter,
    ibge_lookup: dict[str, dict[str, str]],
    run_id: str,
    stats: dict[str, int],
    max_rows_per_file: int | None,
):
    inner_name = get_inner_csv_name(zip_path)
    delimiter = detect_delimiter(zip_path)
    with zipfile.ZipFile(zip_path, "r") as archive:
        with archive.open(inner_name, "r") as buffer:
            text_stream = (line.decode("latin-1", errors="replace") for line in buffer)
            reader = csv.DictReader(text_stream, delimiter=delimiter, quotechar='"')

            if reader.fieldnames is None:
                return

            normalized_header = [normalize_colname(column) for column in reader.fieldnames]

            for row_number, raw_row in enumerate(reader, start=1):
                if max_rows_per_file is not None and row_number > max_rows_per_file:
                    break

                stats["rows_read"] += 1

                normalized_row: dict[str, str] = {}
                for original_column, normalized_column in zip(reader.fieldnames, normalized_header):
                    if not normalized_column:
                        continue
                    normalized_row[normalized_column] = clean_value(raw_row.get(original_column, ""))

                output_row = {column: normalized_row.get(column, "") for column in union_columns}

                codmunnasc = normalize_municipio_code(normalized_row.get("CODMUNNASC", ""))
                codmunres = normalize_municipio_code(normalized_row.get("CODMUNRES", ""))

                codestab_std, codestab_valid = normalize_codestab(normalized_row.get("CODESTAB", ""))
                if codestab_valid == "0":
                    stats["invalid_codestab"] += 1

                nasc_ref = ibge_lookup.get(codmunnasc, {})
                res_ref = ibge_lookup.get(codmunres, {})

                missing_count = sum(1 for value in output_row.values() if value == "")

                output_row.update(
                    {
                        "TRANSFORM_VERSION": ETL_VERSION,
                        "RUN_ID": run_id,
                        "YEAR": str(year),
                        "SOURCE_ZIP": str(zip_path),
                        "SOURCE_ROW_NUMBER": str(row_number),
                        "CODMUNNASC_STD": codmunnasc,
                        "CODMUNRES_STD": codmunres,
                        "CODMUNNASC_UF": nasc_ref.get("uf", ""),
                        "CODMUNRES_UF": res_ref.get("uf", ""),
                        "CODMUNNASC_NOME": nasc_ref.get("municipio", ""),
                        "CODMUNRES_NOME": res_ref.get("municipio", ""),
                        "CODESTAB_STD": codestab_std,
                        "CODESTAB_VALID": codestab_valid,
                        "DTNASC_ISO": parse_date_ddmmyyyy(normalized_row.get("DTNASC", "")),
                        "IDADEMAE_INT": parse_int(normalized_row.get("IDADEMAE", ""), min_value=10, max_value=70),
                        "PESO_INT": parse_int(normalized_row.get("PESO", ""), min_value=100, max_value=9999),
                        "APGAR1_INT": parse_int(normalized_row.get("APGAR1", ""), min_value=0, max_value=10),
                        "APGAR5_INT": parse_int(normalized_row.get("APGAR5", ""), min_value=0, max_value=10),
                        "CONSPRENAT_INT": parse_int(normalized_row.get("CONSPRENAT", ""), min_value=0, max_value=99),
                        "SEXO_DESC": category_label("SEXO", normalized_row.get("SEXO", "")),
                        "RACACOR_DESC": category_label("RACACOR", normalized_row.get("RACACOR", "")),
                        "PARTO_DESC": category_label("PARTO", normalized_row.get("PARTO", "")),
                        "MISSING_COUNT": str(missing_count),
                    }
                )

                writer.writerow(output_row)
                stats["rows_written"] += 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline ETL do SINASC (Fase 3)")
    parser.add_argument("--raw-dir", default="data/raw", help="Diretório com os ZIPs brutos")
    parser.add_argument("--processed-dir", default="data/processed", help="Diretório de saída")
    parser.add_argument("--start-year", type=int, default=MIN_YEAR, help=f"Ano inicial (mínimo {MIN_YEAR})")
    parser.add_argument("--end-year", type=int, default=None, help="Ano final opcional")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescreve o arquivo harmonizado caso já exista",
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=None,
        help="Limite de linhas por arquivo (útil para testes rápidos)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.start_year is not None and args.start_year < MIN_YEAR:
        raise ValueError(f"Ano inicial inválido: {args.start_year}. Use ano >= {MIN_YEAR}.")
    if args.end_year is not None and args.end_year < MIN_YEAR:
        raise ValueError(f"Ano final inválido: {args.end_year}. Use ano >= {MIN_YEAR}.")
    if args.end_year is not None and args.start_year is not None and args.end_year < args.start_year:
        raise ValueError("Intervalo inválido: --end-year deve ser maior ou igual a --start-year.")

    started_at = now_utc_iso()
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)
    logs_dir = processed_dir / "logs"
    reference_dir = Path("data/reference")

    processed_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    files = list_input_files(raw_dir, args.start_year, args.end_year)
    if not files:
        raise RuntimeError("Nenhum arquivo de entrada encontrado para o intervalo informado.")

    print(f"[INFO] Arquivos de entrada: {len(files)}")

    union_columns = build_union_columns(files)
    print(f"[INFO] Colunas harmonizadas detectadas: {len(union_columns)}")

    ibge_lookup = fetch_ibge_municipios(reference_dir)
    print(f"[INFO] Municípios IBGE carregados: {len(ibge_lookup)}")

    category_path = write_category_config(processed_dir)

    output_file = processed_dir / "sinasc_harmonized.csv"
    if output_file.exists() and not args.overwrite:
        raise RuntimeError(
            f"Arquivo de saída já existe: {output_file}. Use --overwrite para substituir."
        )

    full_columns = union_columns + DERIVED_COLUMNS

    stats = {
        "files_processed": 0,
        "rows_read": 0,
        "rows_written": 0,
        "invalid_codestab": 0,
    }

    with output_file.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=full_columns)
        writer.writeheader()

        for year, zip_path in files:
            print(f"[INFO] Processando {zip_path}")
            process_zip_file(
                year=year,
                zip_path=zip_path,
                union_columns=union_columns,
                writer=writer,
                ibge_lookup=ibge_lookup,
                run_id=run_id,
                stats=stats,
                max_rows_per_file=args.max_rows_per_file,
            )
            stats["files_processed"] += 1

    columns_path = processed_dir / "sinasc_harmonized_columns.json"
    columns_path.write_text(
        json.dumps(
            {
                "etl_version": ETL_VERSION,
                "generated_at": now_utc_iso(),
                "columns_raw_harmonized": union_columns,
                "columns_derived": DERIVED_COLUMNS,
                "total_columns": len(full_columns),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    finished_at = now_utc_iso()

    summary = {
        "run_id": run_id,
        "etl_version": ETL_VERSION,
        "started_at": started_at,
        "finished_at": finished_at,
        "raw_dir": str(raw_dir),
        "processed_dir": str(processed_dir),
        "start_year": args.start_year,
        "end_year": args.end_year,
        "output_file": str(output_file),
        "columns_file": str(columns_path),
        "category_config_file": str(category_path),
        "stats": stats,
    }

    summary_path = logs_dir / f"etl_run_{run_id}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    latest_path = logs_dir / "etl_latest.json"
    latest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    history_path = logs_dir / "etl_runs.jsonl"
    with history_path.open("a", encoding="utf-8") as history:
        history.write(json.dumps(summary, ensure_ascii=False) + "\n")

    print(f"[OK] ETL concluído. Saída harmonizada: {output_file}")
    print(f"[OK] Logs: {summary_path}")


if __name__ == "__main__":
    main()