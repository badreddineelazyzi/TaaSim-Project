#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve
from urllib.parse import urlparse


DEFAULT_KAGGLE_COMPETITION = "pkdd-15-predict-taxi-service-trajectory-i"
DEFAULT_NYC_MONTHS = ["2024-01", "2024-02", "2024-03"]
DEFAULT_DOWNLOAD_ROOT = Path("data/downloads")
DEFAULT_MINIO_ALIAS = "local"
DEFAULT_MINIO_ENDPOINT = "http://minio:9000"
DEFAULT_MINIO_ACCESS_KEY = "minioadmin"
DEFAULT_MINIO_SECRET_KEY = "minioadmin123"
DEFAULT_ML_BUCKET = "ml-data"
DEFAULT_MC_IMAGE = "minio/mc:latest"


def load_dotenv(dotenv_path: Path = Path(".env")) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def getenv(name: str, default: str) -> str:
    return os.environ.get(name, default)


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Download datasets and upload them to MinIO.")
    parser.add_argument("--kaggle-competition", default=getenv("KAGGLE_COMPETITION", DEFAULT_KAGGLE_COMPETITION))
    parser.add_argument("--nyc-months", nargs="*", default=os.environ.get("NYC_MONTHS", " ").split() if os.environ.get("NYC_MONTHS") else DEFAULT_NYC_MONTHS)
    parser.add_argument("--download-root", default=getenv("DOWNLOAD_ROOT", str(DEFAULT_DOWNLOAD_ROOT)))
    parser.add_argument("--minio-alias", default=getenv("MINIO_ALIAS", DEFAULT_MINIO_ALIAS))
    parser.add_argument("--minio-endpoint", default=getenv("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT))
    parser.add_argument("--minio-access-key", default=getenv("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY))
    parser.add_argument("--minio-secret-key", default=getenv("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY))
    parser.add_argument("--ml-bucket", default=getenv("ML_BUCKET", DEFAULT_ML_BUCKET))
    parser.add_argument("--mc-image", default=getenv("MC_IMAGE", DEFAULT_MC_IMAGE))
    parser.add_argument("--quick-mode", action="store_true", help="Use tiny local sample datasets instead of full internet downloads.")
    return parser.parse_args()


def run_command(command: list[str], message: str) -> None:
    print(message, flush=True)
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode != 0:
        command_text = " ".join(command)
        if "kaggle" in command_text.lower() and "403" in (completed.stderr or completed.stdout or ""):
            raise SystemExit(
                "Kaggle returned 403 for the Porto competition download. "
                "Usually this means the Kaggle account is authenticated but has not joined the competition yet. "
                "Open the competition page, accept the rules, then rerun the loader."
            )
        raise SystemExit(f"Command failed ({completed.returncode}): {' '.join(command)}")


def run_mc(args: list[str], message: str, mounts: list[str], mc_image: str) -> None:
    minio_alias = os.environ.get("MINIO_ALIAS", DEFAULT_MINIO_ALIAS)
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY)
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY)
    parsed_endpoint = urlparse(minio_endpoint)
    endpoint_host = parsed_endpoint.netloc or parsed_endpoint.path
    mc_host_value = f"{parsed_endpoint.scheme or 'http'}://{minio_access_key}:{minio_secret_key}@{endpoint_host}"
    command = ["docker", "run", "--rm", "--network", "taasim-net"]
    for mount in mounts:
        command.extend(["-v", mount])
    command.extend(["-e", f"MC_HOST_{minio_alias}={mc_host_value}", mc_image])
    command.extend(args)
    run_command(command, message)


def run_kaggle(command_suffix: list[str], message: str) -> None:
    kaggle_cli = shutil.which("kaggle")
    if kaggle_cli is not None:
        run_command([kaggle_cli, *command_suffix], message)
        return

    run_command([sys.executable, "-m", "kaggle", *command_suffix], message)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def reset_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def get_csv_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.csv"))


def create_porto_sample_csv(porto_root: Path) -> None:
    sample_file = porto_root / "porto_sample.csv"
    rows: list[dict[str, object]] = []
    for i in range(1, 101):
        polyline = [
            [-8.62 + (i * 0.00005), 41.15 + (i * 0.00004)],
            [-8.619 + (i * 0.00005), 41.151 + (i * 0.00004)],
            [-8.618 + (i * 0.00005), 41.152 + (i * 0.00004)],
        ]
        rows.append(
            {
                "TRIP_ID": f"SAMPLE_TRIP_{i}",
                "CALL_TYPE": "A" if i % 3 == 0 else "B",
                "ORIGIN_CALL": "",
                "ORIGIN_STAND": "",
                "TAXI_ID": str(200000 + i),
                "TIMESTAMP": 1408032000 + (i * 60),
                "DAY_TYPE": "A",
                "MISSING_DATA": "False",
                "POLYLINE": json.dumps(polyline),
            }
        )

    header = [
        "TRIP_ID",
        "CALL_TYPE",
        "ORIGIN_CALL",
        "ORIGIN_STAND",
        "TAXI_ID",
        "TIMESTAMP",
        "DAY_TYPE",
        "MISSING_DATA",
        "POLYLINE",
    ]
    with sample_file.open("w", encoding="utf-8", newline="") as f:
        f.write(",".join(header) + "\n")
        for row in rows:
            escaped = []
            for key in header:
                value = str(row[key]).replace('"', '""')
                escaped.append(f'"{value}"')
            f.write(",".join(escaped) + "\n")


def create_nyc_sample_parquet(nyc_root: Path, months: list[str]) -> None:
    try:
        import pandas as pd
    except ImportError as exc:
        raise SystemExit(
            "Quick mode needs pandas + pyarrow. "
            f"Import error: {exc}. Install with: python -m pip install pandas pyarrow"
        ) from exc

    for month in months:
        sample_path = nyc_root / f"yellow_tripdata_{month}.parquet"
        if sample_path.exists():
            continue

        records = []
        for i in range(1, 151):
            records.append(
                {
                    "VendorID": 1,
                    "tpep_pickup_datetime": f"{month}-{(i % 28) + 1:02d} 08:{(i % 60):02d}:00",
                    "tpep_dropoff_datetime": f"{month}-{(i % 28) + 1:02d} 08:{((i + 12) % 60):02d}:00",
                    "passenger_count": (i % 4) + 1,
                    "trip_distance": round(1.2 + ((i % 15) * 0.35), 3),
                    "PULocationID": (i % 250) + 1,
                    "DOLocationID": ((i + 20) % 250) + 1,
                    "fare_amount": round(5.5 + ((i % 20) * 1.15), 2),
                    "total_amount": round(8.0 + ((i % 20) * 1.35), 2),
                }
            )

        pd.DataFrame.from_records(records).to_parquet(sample_path, index=False)


def main() -> None:
    args = parse_args()
    download_root = Path(args.download_root)
    porto_root = download_root / "porto"
    nyc_root = download_root / "nyc-tlc"
    porto_upload_root = download_root / "porto-upload"
    nyc_upload_root = download_root / "nyc-upload"

    ensure_directory(download_root)
    ensure_directory(porto_root)
    ensure_directory(nyc_root)

    if shutil.which("docker") is None:
        raise SystemExit("Docker is required but was not found in PATH.")

    try:
        import kaggle  # noqa: F401
    except ImportError as exc:
        raise SystemExit("Kaggle package is required. Install it and configure kaggle.json first.") from exc

    kaggle_username = os.environ.get("KAGGLE_USERNAME")
    kaggle_key = os.environ.get("KAGGLE_KEY")
    if kaggle_username and kaggle_key:
        kaggle_dir = Path.home() / ".kaggle"
        ensure_directory(kaggle_dir)
        kaggle_config = kaggle_dir / "kaggle.json"
        if not kaggle_config.exists():
            kaggle_config.write_text(
                f'{{"username": "{kaggle_username}", "key": "{kaggle_key}"}}',
                encoding="utf-8",
            )

    for bucket in ("raw", "curated", args.ml_bucket, "kafka-archive"):
        run_mc(["mb", "-p", f"{args.minio_alias}/{bucket}"], f"Ensuring bucket: {bucket}", [], args.mc_image)

    if args.quick_mode:
        print("Quick mode enabled: creating local sample datasets.", flush=True)
        if not get_csv_files(porto_root):
            create_porto_sample_csv(porto_root)
    elif not get_csv_files(porto_root):
        run_kaggle(["competitions", "download", "-c", args.kaggle_competition, "-p", str(porto_root)], "Downloading Porto competition dataset from Kaggle")

        for zip_path in sorted(porto_root.glob("*.zip")):
            print(f"Extracting {zip_path.name}", flush=True)
            with zipfile.ZipFile(zip_path) as archive:
                archive.extractall(porto_root)
            zip_path.unlink()

    porto_csv_files = get_csv_files(porto_root)
    if not porto_csv_files:
        raise SystemExit("No Porto CSV files were found after extraction. Check the Kaggle download and competition access.")

    reset_directory(porto_upload_root)
    for csv_file in porto_csv_files:
        shutil.copy2(csv_file, porto_upload_root / csv_file.name)

    if args.quick_mode:
        create_nyc_sample_parquet(nyc_root, args.nyc_months)

    nyc_parquet_files: list[Path] = []
    for month in args.nyc_months:
        nyc_file = nyc_root / f"yellow_tripdata_{month}.parquet"
        if not nyc_file.exists():
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
            print(f"Downloading NYC TLC parquet for {month}", flush=True)
            urlretrieve(url, nyc_file)
        nyc_parquet_files.append(nyc_file)

    if not nyc_parquet_files:
        raise SystemExit("No NYC TLC parquet files were downloaded.")

    reset_directory(nyc_upload_root)
    for parquet_file in nyc_parquet_files:
        shutil.copy2(parquet_file, nyc_upload_root / parquet_file.name)

    porto_mount = f"{porto_upload_root.resolve()}:/data/porto-upload"
    nyc_mount = f"{nyc_upload_root.resolve()}:/data/nyc-upload"

    run_mc(
        ["mirror", "--overwrite", "/data/porto-upload", f"{args.minio_alias}/raw/porto-trips/"],
        "Uploading Porto CSV files to MinIO",
        [porto_mount],
        args.mc_image,
    )
    run_mc(
        ["mirror", "--overwrite", "/data/nyc-upload", f"{args.minio_alias}/raw/nyc-tlc/"],
        "Uploading NYC TLC parquet files to MinIO",
        [nyc_mount],
        args.mc_image,
    )

    print("Verifying uploads", flush=True)
    run_mc(["ls", f"{args.minio_alias}/raw/"], "Listing raw bucket", [], args.mc_image)
    run_mc(["ls", f"{args.minio_alias}/raw/porto-trips/"], "Listing Porto prefix", [], args.mc_image)
    run_mc(["ls", f"{args.minio_alias}/raw/nyc-tlc/"], "Listing NYC prefix", [], args.mc_image)

    print("Dataset load completed successfully.")


if __name__ == "__main__":
    main()