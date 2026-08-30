#!/usr/bin/env python3
"""
CMS Provider Data - Hospitals incremental downloader.

What it does:
1. Reads the CMS Provider Data metastore.
2. Selects every dataset whose theme contains "Hospitals".
3. Finds CSV distributions for those datasets.
4. Uses a local SQLite database to determine which files are new/modified.
5. Downloads and converts CSV headers to snake_case in parallel.
6. Records run-level and file-level audit metadata.

Uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen


METASTORE_URL = (
    "https://data.cms.gov/provider-data/api/1/"
    "metastore/schemas/dataset/items"
)
THEME = "Hospitals"
DEFAULT_WORKERS = 8
HTTP_TIMEOUT_SECONDS = 90
HTTP_RETRIES = 3
USER_AGENT = "cms-hospital-assessment/1.0"


@dataclass(frozen=True)
class WorkItem:
    dataset_id: str
    title: str
    distribution_name: str
    source_url: str
    source_modified: str | None


@dataclass
class ProcessResult:
    item: WorkItem
    status: str
    output_path: str | None = None
    row_count: int | None = None
    file_size_bytes: int | None = None
    error: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def snake_case(value: str) -> str:
    """
    Convert a column name to snake_case.

    Example:
    "Patients’ rating of the facility linear mean score"
      -> "patients_rating_of_the_facility_linear_mean_score"
    """
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unnamed_column"


def make_unique_headers(headers: list[str]) -> list[str]:
    """
    Snake-case headers and make duplicates deterministic:
    name, name -> name, name_2
    """
    seen: dict[str, int] = {}
    result: list[str] = []

    for header in headers:
        base = snake_case(header)
        seen[base] = seen.get(base, 0) + 1
        suffix = seen[base]
        result.append(base if suffix == 1 else f"{base}_{suffix}")

    return result


def safe_output_stem(filename: str) -> str:
    stem = Path(filename).stem
    stem = snake_case(stem)
    return stem or "dataset"


def filename_from_url(url: str) -> str:
    path = unquote(urlparse(url).path)
    filename = Path(path).name
    return filename or "dataset.csv"


def http_get_json(url: str) -> Any:
    last_error: Exception | None = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            request = Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.load(response)
        except Exception as exc:
            last_error = exc
            if attempt == HTTP_RETRIES:
                break
            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(f"Unable to read CMS metastore: {last_error}") from last_error


def discover_hospital_csvs() -> list[WorkItem]:
    datasets = http_get_json(METASTORE_URL)
    work_items: list[WorkItem] = []

    for dataset in datasets:
        themes = dataset.get("theme") or []
        if not any(str(theme).casefold() == THEME.casefold() for theme in themes):
            continue

        dataset_id = str(dataset.get("identifier", "")).strip()
        if not dataset_id:
            continue

        title = str(dataset.get("title", dataset_id))
        modified = dataset.get("modified")
        modified = str(modified) if modified is not None else None

        for distribution in dataset.get("distribution") or []:
            url = distribution.get("downloadURL")
            media_type = str(distribution.get("mediaType", "")).lower()

            if not url:
                continue

            # The assessment asks for CSV files. Accept either an explicit
            # CSV media type or a .csv URL.
            if "csv" not in media_type and not urlparse(url).path.lower().endswith(".csv"):
                continue

            distribution_name = filename_from_url(url)
            work_items.append(
                WorkItem(
                    dataset_id=dataset_id,
                    title=title,
                    distribution_name=distribution_name,
                    source_url=str(url),
                    source_modified=modified,
                )
            )

    # Deterministic ordering makes logs and tests easier to compare.
    return sorted(
        work_items,
        key=lambda x: (x.dataset_id, x.distribution_name.casefold()),
    )


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL,
            discovered_files INTEGER NOT NULL DEFAULT 0,
            selected_files INTEGER NOT NULL DEFAULT 0,
            succeeded_files INTEGER NOT NULL DEFAULT 0,
            failed_files INTEGER NOT NULL DEFAULT 0,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS dataset_state (
            dataset_id TEXT NOT NULL,
            distribution_name TEXT NOT NULL,
            title TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_modified TEXT,
            last_successful_run_id TEXT NOT NULL,
            last_successful_at TEXT NOT NULL,
            output_path TEXT NOT NULL,
            row_count INTEGER,
            file_size_bytes INTEGER,
            PRIMARY KEY (dataset_id, distribution_name)
        );

        CREATE TABLE IF NOT EXISTS run_files (
            run_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            distribution_name TEXT NOT NULL,
            title TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_modified TEXT,
            status TEXT NOT NULL,
            output_path TEXT,
            row_count INTEGER,
            file_size_bytes INTEGER,
            error TEXT,
            PRIMARY KEY (run_id, dataset_id, distribution_name),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
        """
    )
    conn.commit()
    return conn


def current_state(conn: sqlite3.Connection) -> dict[tuple[str, str], sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT dataset_id, distribution_name, source_url, source_modified
        FROM dataset_state
        """
    ).fetchall()

    return {
        (row["dataset_id"], row["distribution_name"]): row
        for row in rows
    }


def needs_download(
    item: WorkItem,
    state: sqlite3.Row | None,
    force: bool = False,
) -> bool:
    if force or state is None:
        return True

    # If CMS changes the resource URL, treat it as a new version even if
    # the catalog's modified date happens to stay the same.
    if item.source_url != state["source_url"]:
        return True

    old_modified = state["source_modified"]
    new_modified = item.source_modified

    # If CMS does not provide a modified value, re-download conservatively
    # because there is no reliable incremental watermark.
    if not new_modified:
        return True

    # CMS currently uses ISO-style dates (YYYY-MM-DD). We intentionally
    # process any changed watermark rather than only "greater than" so an
    # upstream metadata correction cannot cause a version to be missed.
    return new_modified != old_modified


def download_and_process(item: WorkItem, output_dir: Path) -> ProcessResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    output_name = (
        f"{snake_case(item.dataset_id)}__"
        f"{safe_output_stem(item.distribution_name)}.csv"
    )
    output_path = output_dir / output_name
    temp_path = output_path.with_name(
        f".{output_path.name}.{uuid.uuid4().hex}.tmp"
    )

    last_error: Exception | None = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            request = Request(
                item.source_url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/csv,*/*;q=0.8",
                },
            )

            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                # utf-8-sig removes a UTF-8 BOM when present.
                # errors="replace" prevents one unusual source character from
                # failing an otherwise valid file.
                text_stream = io.TextIOWrapper(
                    response,
                    encoding="utf-8-sig",
                    errors="replace",
                    newline="",
                )
                reader = csv.reader(text_stream)

                try:
                    original_headers = next(reader)
                except StopIteration as exc:
                    raise ValueError("Downloaded CSV is empty") from exc

                normalized_headers = make_unique_headers(original_headers)

                row_count = 0
                with temp_path.open(
                    "w",
                    encoding="utf-8",
                    newline="",
                ) as output_file:
                    writer = csv.writer(output_file, lineterminator="\n")
                    writer.writerow(normalized_headers)

                    for row in reader:
                        writer.writerow(row)
                        row_count += 1

            # Atomic replacement: the final filename is never a partial file.
            os.replace(temp_path, output_path)
            file_size = output_path.stat().st_size

            return ProcessResult(
                item=item,
                status="SUCCESS",
                output_path=str(output_path.resolve()),
                row_count=row_count,
                file_size_bytes=file_size,
            )

        except Exception as exc:
            last_error = exc
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass

            if attempt < HTTP_RETRIES:
                time.sleep(2 ** (attempt - 1))

    return ProcessResult(
        item=item,
        status="FAILED",
        error=f"{type(last_error).__name__}: {last_error}",
    )


def record_result(
    conn: sqlite3.Connection,
    run_id: str,
    result: ProcessResult,
) -> None:
    item = result.item

    conn.execute(
        """
        INSERT OR REPLACE INTO run_files (
            run_id,
            dataset_id,
            distribution_name,
            title,
            source_url,
            source_modified,
            status,
            output_path,
            row_count,
            file_size_bytes,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            item.dataset_id,
            item.distribution_name,
            item.title,
            item.source_url,
            item.source_modified,
            result.status,
            result.output_path,
            result.row_count,
            result.file_size_bytes,
            result.error,
        ),
    )

    # Important: only advance the incremental watermark after success.
    if result.status == "SUCCESS":
        conn.execute(
            """
            INSERT INTO dataset_state (
                dataset_id,
                distribution_name,
                title,
                source_url,
                source_modified,
                last_successful_run_id,
                last_successful_at,
                output_path,
                row_count,
                file_size_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset_id, distribution_name) DO UPDATE SET
                title = excluded.title,
                source_url = excluded.source_url,
                source_modified = excluded.source_modified,
                last_successful_run_id = excluded.last_successful_run_id,
                last_successful_at = excluded.last_successful_at,
                output_path = excluded.output_path,
                row_count = excluded.row_count,
                file_size_bytes = excluded.file_size_bytes
            """,
            (
                item.dataset_id,
                item.distribution_name,
                item.title,
                item.source_url,
                item.source_modified,
                run_id,
                utc_now(),
                result.output_path,
                result.row_count,
                result.file_size_bytes,
            ),
        )

    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'Incrementally download CMS Provider Data CSV datasets '
            'with theme "Hospitals" and normalize headers to snake_case.'
        )
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for processed CSV files (default: output)",
    )
    parser.add_argument(
        "--metadata-db",
        default="cms_hospital_metadata.db",
        help="SQLite metadata database (default: cms_hospital_metadata.db)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel worker count (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all Hospital CSVs regardless of metadata state",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )

    output_dir = Path(args.output_dir).expanduser().resolve()
    db_path = Path(args.metadata_db).expanduser().resolve()
    run_id = uuid.uuid4().hex
    started_at = utc_now()

    conn = connect_db(db_path)
    conn.execute(
        """
        INSERT INTO runs (run_id, started_at, status)
        VALUES (?, ?, 'RUNNING')
        """,
        (run_id, started_at),
    )
    conn.commit()

    try:
        discovered = discover_hospital_csvs()
        state = current_state(conn)

        selected = [
            item
            for item in discovered
            if needs_download(
                item,
                state.get((item.dataset_id, item.distribution_name)),
                force=args.force,
            )
        ]

        conn.execute(
            """
            UPDATE runs
            SET discovered_files = ?, selected_files = ?
            WHERE run_id = ?
            """,
            (len(discovered), len(selected), run_id),
        )
        conn.commit()

        logging.info(
            "Discovered %d Hospital CSV distribution(s); %d require processing.",
            len(discovered),
            len(selected),
        )

        if not selected:
            conn.execute(
                """
                UPDATE runs
                SET completed_at = ?, status = 'SUCCESS'
                WHERE run_id = ?
                """,
                (utc_now(), run_id),
            )
            conn.commit()
            logging.info("No CMS Hospital files changed since the previous successful run.")
            return 0

        results: list[ProcessResult] = []

        with ThreadPoolExecutor(
            max_workers=args.workers,
            thread_name_prefix="cms-worker",
        ) as executor:
            future_to_item = {
                executor.submit(download_and_process, item, output_dir): item
                for item in selected
            }

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                except Exception as exc:
                    # Defensive catch: download_and_process already converts
                    # expected failures into ProcessResult.
                    result = ProcessResult(
                        item=item,
                        status="FAILED",
                        error=f"{type(exc).__name__}: {exc}",
                    )

                results.append(result)
                record_result(conn, run_id, result)

                if result.status == "SUCCESS":
                    logging.info(
                        "SUCCESS %s | rows=%s | %s",
                        item.dataset_id,
                        result.row_count,
                        item.title,
                    )
                else:
                    logging.error(
                        "FAILED %s | %s | %s",
                        item.dataset_id,
                        item.title,
                        result.error,
                    )

        succeeded = sum(r.status == "SUCCESS" for r in results)
        failed = sum(r.status == "FAILED" for r in results)
        final_status = "SUCCESS" if failed == 0 else "PARTIAL_FAILURE"

        conn.execute(
            """
            UPDATE runs
            SET completed_at = ?,
                status = ?,
                succeeded_files = ?,
                failed_files = ?
            WHERE run_id = ?
            """,
            (utc_now(), final_status, succeeded, failed, run_id),
        )
        conn.commit()

        logging.info(
            "Run complete. status=%s succeeded=%d failed=%d metadata=%s",
            final_status,
            succeeded,
            failed,
            db_path,
        )
        return 0 if failed == 0 else 1

    except Exception as exc:
        conn.execute(
            """
            UPDATE runs
            SET completed_at = ?, status = 'FAILED', error = ?
            WHERE run_id = ?
            """,
            (utc_now(), f"{type(exc).__name__}: {exc}", run_id),
        )
        conn.commit()
        logging.exception("Job failed")
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

