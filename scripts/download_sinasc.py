import argparse
import json
from pathlib import Path
import requests

# Base pública do Portal de Dados Abertos do SUS (S3)
# Exemplo real: SINASC_2023_csv.zip aparece com essa URL no portal. (2023) :contentReference[oaicite:1]{index=1}
BASE_S3 = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv"

def download_file(url: str, out_path: Path, timeout: int = 180) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        return "skip"

    try:
        r = requests.get(url, stream=True, timeout=timeout)
        if r.status_code != 200:
            return f"error_http_{r.status_code}"

        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        if out_path.stat().st_size == 0:
            return "error_empty"

        return "ok"
    except requests.exceptions.ConnectTimeout:
        return "error_connecttimeout"
    except requests.exceptions.ReadTimeout:
        return "error_readtimeout"
    except requests.exceptions.ConnectionError:
        return "error_connection"
    except Exception as e:
        return f"error_{e.__class__.__name__}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw", help="pasta de saída (default: data/raw)")
    ap.add_argument("--start-year", type=int, required=True)
    ap.add_argument("--end-year", type=int, required=True)
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "source": BASE_S3,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "files": []
    }

    for year in range(args.start_year, args.end_year + 1):
        # padrão do portal: SINASC_YYYY_csv.zip (ex: SINASC_2023_csv.zip) :contentReference[oaicite:2]{index=2}
        fname = f"SINASC_{year}_csv.zip"
        url = f"{BASE_S3}/{fname}"
        out_path = out_dir / str(year) / fname

        status = download_file(url, out_path)
        print(f"[{status.upper()}] {year} -> {out_path}", flush=True)

        manifest["files"].append({
            "year": year,
            "url": url,
            "saved_as": str(out_path),
            "status": status
        })

    manifest_path = out_dir / "download_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] Manifest salvo em: {manifest_path}", flush=True)

if __name__ == "__main__":
    main()