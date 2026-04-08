from __future__ import annotations

import csv
import traceback
from pathlib import Path

from openpyxl import Workbook
from openpyxl.utils.exceptions import IllegalCharacterError
from rdflib import Graph

from .io_utils import atomic_write_bytes, atomic_write_path


def csv_file_to_xlsx(csv_path: Path, xlsx_path: Path, delimiter: str = ";") -> None:
    def _write(tmp_path: Path) -> None:
        wb = Workbook(write_only=True)
        ws = wb.create_sheet(title="data")

        try_encs = ["utf-8-sig", "cp1250", "latin2"]
        last_err: Exception | None = None

        for enc in try_encs:
            try:
                with csv_path.open("r", encoding=enc, newline="") as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    for row in reader:
                        ws.append(row)
                last_err = None
                break
            except UnicodeDecodeError as e:
                last_err = e

        if last_err:
            raise last_err

        wb.save(tmp_path)

    try:
        atomic_write_path(xlsx_path, _write)
    except (IllegalCharacterError, Exception) as e:
        print(f"[xlsx] FAILED for {csv_path}: {type(e).__name__}: {e}")


def rdfxml_file_to_jsonld(xml_path: Path, jsonld_path: Path) -> None:
    g = Graph()
    try:
        g.parse(str(xml_path), format="application/rdf+xml")
        data = g.serialize(format="json-ld", indent=2)
        if isinstance(data, bytes):
            atomic_write_bytes(jsonld_path, data)
        else:
            atomic_write_bytes(jsonld_path, data.encode("utf-8"))
    except Exception as e:
        print(f"[jsonld] FAILED for {xml_path}: {type(e).__name__}: {e}")
        err_path = jsonld_path.with_suffix(jsonld_path.suffix + ".error.txt")
        err_text = (
            f"Failed to convert RDF/XML -> JSON-LD\n"
            f"xml: {xml_path}\n"
            f"error: {type(e).__name__}: {e}\n\n"
            f"traceback:\n{traceback.format_exc()}\n"
        )
        atomic_write_bytes(err_path, err_text.encode("utf-8"))
