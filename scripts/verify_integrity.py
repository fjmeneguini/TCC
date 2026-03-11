import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from urllib import request
import zipfile


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

RAW_FIELDS_TO_CHECK = [
    "DTNASC",
    "IDADEMAE",
    "PESO",
    "APGAR1",
    "APGAR5",
    "CONSPRENAT",
    "SEXO",
    "RACACOR",
    "PARTO",
    "CODMUNNASC",
    "CODMUNRES",
    "CODESTAB",
]

BEFORE_AFTER_MAP = {
    "DTNASC": "DTNASC_ISO",
    "IDADEMAE": "IDADEMAE_INT",
    "PESO": "PESO_INT",
    "APGAR1": "APGAR1_INT",
    "APGAR5": "APGAR5_INT",
    "CONSPRENAT": "CONSPRENAT_INT",
}


def now_utc_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def clean_value(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.upper() in MISSING_MARKERS:
        return ""
    return text


def normalize_colname(column_name: str) -> str:
    normalized = str(column_name).strip().upper()
    if normalized.startswith("\ufeff"):
        normalized = normalized.replace("\ufeff", "", 1)
    return normalized


def get_inner_csv_name(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path, "r") as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"Arquivo sem CSV interno: {zip_path}")
        return csv_names[0]


def detect_delimiter(zip_path: Path) -> str:
    inner_name = get_inner_csv_name(zip_path)
    with zipfile.ZipFile(zip_path, "r") as archive:
        with archive.open(inner_name, "r") as buffer:
            first_line = buffer.readline().decode("latin-1", errors="replace")
    return ";" if first_line.count(";") >= first_line.count(",") else ","


def init_missing_accumulator(fields: list[str]) -> dict[str, dict[str, int]]:
    return {field: {"empty": 0, "missing_marker": 0} for field in fields}


def analyze_raw_zips(raw_dir: Path) -> dict:
    zip_files: list[tuple[int, Path]] = []
    for year_dir in sorted(raw_dir.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        matches = sorted(year_dir.glob("SINASC_*_csv.zip"))
        for item in matches:
            zip_files.append((year, item))

    rows_total = 0
    rows_by_year: dict[str, int] = {}
    missing = init_missing_accumulator(RAW_FIELDS_TO_CHECK)
    zip_integrity: list[dict[str, str]] = []

    for year, zip_path in zip_files:
        rows_by_year.setdefault(str(year), 0)
        delimiter = detect_delimiter(zip_path)
        inner_name = get_inner_csv_name(zip_path)

        with zipfile.ZipFile(zip_path, "r") as archive:
            bad_file = archive.testzip()
            zip_integrity.append(
                {
                    "year": str(year),
                    "zip_path": str(zip_path),
                    "status": "ok" if bad_file is None else "corrupted",
                    "bad_member": bad_file or "",
                }
            )

            with archive.open(inner_name, "r") as buffer:
                text_stream = (line.decode("latin-1", errors="replace") for line in buffer)
                reader = csv.DictReader(text_stream, delimiter=delimiter, quotechar='"')

                if reader.fieldnames is None:
                    continue

                field_map = {name: normalize_colname(name) for name in reader.fieldnames}

                for row in reader:
                    rows_total += 1
                    rows_by_year[str(year)] += 1

                    normalized_row: dict[str, str] = {}
                    for original_name, normalized_name in field_map.items():
                        if not normalized_name:
                            continue
                        normalized_row[normalized_name] = row.get(original_name, "")

                    for field in RAW_FIELDS_TO_CHECK:
                        original = normalized_row.get(field, "")
                        stripped = "" if original is None else str(original).strip()
                        if stripped == "":
                            missing[field]["empty"] += 1
                        if clean_value(original) == "":
                            missing[field]["missing_marker"] += 1

    return {
        "files": [{"year": str(year), "zip_path": str(path)} for year, path in zip_files],
        "rows_total": rows_total,
        "rows_by_year": rows_by_year,
        "missing": missing,
        "zip_integrity": zip_integrity,
    }


def analyze_harmonized_csv(processed_csv: Path) -> dict:
    rows_total = 0
    rows_by_year: dict[str, int] = {}

    treated_fields = list(BEFORE_AFTER_MAP.values())
    tracked_fields = RAW_FIELDS_TO_CHECK + treated_fields
    missing = init_missing_accumulator(tracked_fields)

    with processed_csv.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        for row in reader:
            rows_total += 1
            year = str(row.get("YEAR", "")).strip() or ""
            rows_by_year.setdefault(year, 0)
            rows_by_year[year] += 1

            for field in tracked_fields:
                original = row.get(field, "")
                stripped = "" if original is None else str(original).strip()
                if stripped == "":
                    missing[field]["empty"] += 1
                if clean_value(original) == "":
                    missing[field]["missing_marker"] += 1

    before_after = {}
    for raw_field, treated_field in BEFORE_AFTER_MAP.items():
        before_missing = missing[raw_field]["missing_marker"]
        after_missing = missing[treated_field]["missing_marker"]
        before_after[raw_field] = {
            "treated_field": treated_field,
            "missing_before": before_missing,
            "missing_after": after_missing,
            "delta_after_minus_before": after_missing - before_missing,
        }

    return {
        "rows_total": rows_total,
        "rows_by_year": rows_by_year,
        "missing": missing,
        "before_after_missing": before_after,
    }


def check_download_manifest(manifest_path: Path, base_dir: Path, check_remote_size: bool, timeout: int) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest.get("files", [])

    results = []
    missing_local = 0
    zero_size = 0
    remote_size_mismatch = 0
    remote_size_checked = 0

    for item in files:
        rel = item.get("saved_as", "")
        local_path = base_dir / rel
        exists = local_path.exists()
        size = local_path.stat().st_size if exists else 0

        if not exists:
            missing_local += 1
        if exists and size == 0:
            zero_size += 1

        remote_content_length = None
        size_match_remote = None
        if check_remote_size:
            url = item.get("url", "")
            req = request.Request(url, method="HEAD")
            try:
                with request.urlopen(req, timeout=timeout) as response:
                    cl = response.headers.get("Content-Length")
                    if cl is not None and cl.isdigit():
                        remote_content_length = int(cl)
                        remote_size_checked += 1
                        size_match_remote = exists and (size == remote_content_length)
                        if size_match_remote is False:
                            remote_size_mismatch += 1
            except Exception:
                size_match_remote = None

        results.append(
            {
                "year": item.get("year"),
                "status_manifest": item.get("status"),
                "url": item.get("url"),
                "saved_as": rel,
                "exists_local": exists,
                "size_bytes_local": size,
                "remote_content_length": remote_content_length,
                "size_match_remote": size_match_remote,
            }
        )

    ok_or_skip = sum(1 for item in files if item.get("status") in {"ok", "skip"})

    return {
        "source": manifest.get("source"),
        "start_year": manifest.get("start_year"),
        "end_year": manifest.get("end_year"),
        "total_manifest_files": len(files),
        "ok_or_skip_files": ok_or_skip,
        "missing_local_files": missing_local,
        "zero_size_local_files": zero_size,
        "remote_size_checked_count": remote_size_checked,
        "remote_size_mismatch_count": remote_size_mismatch,
        "files": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verificação de integridade SINASC (download + ETL).")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--processed-file", default="data/processed/sinasc_harmonized.csv")
    parser.add_argument("--manifest", default="data/raw/download_manifest.json")
    parser.add_argument("--output", default="data/processed/logs/integrity_report_latest.json")
    parser.add_argument("--check-remote-size", action="store_true")
    parser.add_argument("--http-timeout", type=int, default=30)
    args = parser.parse_args()

    workspace_root = Path(".").resolve()
    raw_dir = Path(args.raw_dir)
    processed_file = Path(args.processed_file)
    manifest_path = Path(args.manifest)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        raise FileNotFoundError(f"Diretório de dados brutos não encontrado: {raw_dir}")
    if not processed_file.exists():
        raise FileNotFoundError(f"Arquivo harmonizado não encontrado: {processed_file}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifesto de download não encontrado: {manifest_path}")

    download_check = check_download_manifest(
        manifest_path=manifest_path,
        base_dir=workspace_root,
        check_remote_size=args.check_remote_size,
        timeout=args.http_timeout,
    )

    raw_stats = analyze_raw_zips(raw_dir)
    harmonized_stats = analyze_harmonized_csv(processed_file)

    raw_rows = raw_stats["rows_total"]
    harmonized_rows = harmonized_stats["rows_total"]

    report = {
        "generated_at": now_utc_iso(),
        "inputs": {
            "raw_dir": str(raw_dir),
            "processed_file": str(processed_file),
            "manifest": str(manifest_path),
        },
        "download_check": download_check,
        "raw_stats": {
            "rows_total": raw_rows,
            "rows_by_year": raw_stats["rows_by_year"],
            "missing_selected_fields": raw_stats["missing"],
            "zip_integrity": raw_stats["zip_integrity"],
        },
        "harmonized_stats": {
            "rows_total": harmonized_rows,
            "rows_by_year": harmonized_stats["rows_by_year"],
            "missing_selected_fields": harmonized_stats["missing"],
            "before_after_missing": harmonized_stats["before_after_missing"],
        },
        "row_count_comparison": {
            "raw_rows_total": raw_rows,
            "harmonized_rows_total": harmonized_rows,
            "difference": harmonized_rows - raw_rows,
            "exact_match": raw_rows == harmonized_rows,
        },
        "conclusion": {
            "etl_lost_rows": raw_rows != harmonized_rows,
            "download_local_missing_files": download_check["missing_local_files"],
            "download_zero_size_files": download_check["zero_size_local_files"],
            "download_remote_size_mismatch": download_check["remote_size_mismatch_count"],
        },
    }

    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Relatório salvo em: {output_path}")
    print(f"[INFO] Linhas brutas: {raw_rows}")
    print(f"[INFO] Linhas harmonizadas: {harmonized_rows}")
    print(f"[INFO] Diferença (harmonizado - bruto): {harmonized_rows - raw_rows}")
    print(f"[INFO] Correspondência exata: {raw_rows == harmonized_rows}")


if __name__ == "__main__":
    main()
