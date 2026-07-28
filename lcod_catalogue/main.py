from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import quote, unquote

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, FOAF, RDF, XSD

from ces_export.dataset_config import load_config
from ces_export.io_utils import atomic_write_text, load_meta
from ces_export.models import AppConfig, ExportJob, WindowSpec
from ces_export.planner import build_jobs

DCAT = Namespace("http://www.w3.org/ns/dcat#")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
LEG = Namespace("https://data.gov.sk/def/ontology/legislation/")


@dataclass(frozen=True)
class RecordKey:
    dataset: str
    d_from: date
    d_to: date
    data_parent: PurePosixPath


@dataclass(frozen=True)
class GeneratedRecord:
    key: RecordKey
    dataset_iri: URIRef
    record_path: Path
    distribution_paths: tuple[Path, ...]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Generate a static DCAT-AP-SK local open-data catalogue from CES export outputs."
    )
    ap.add_argument("--config", type=Path, required=True, help="CES dataset schedule config")
    ap.add_argument(
        "--metadata-config",
        type=Path,
        required=True,
        help="LKOD catalogue and dataset metadata config",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        help="Root directory containing the exported data; defaults to CES_EXPORT_OUT_DIR",
    )
    ap.add_argument("--today", help="Override today's date (YYYY-MM-DD), mainly for testing")
    ap.add_argument("--start-year", type=int, help="Override schedule start years, as in ces_export")
    ap.add_argument("--end-year", type=int, help="Override schedule end years, as in ces_export")
    ap.add_argument(
        "--allow-missing",
        action="store_true",
        help="Skip planned records for which no current distribution exists",
    )
    return ap.parse_args()


def _need_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _need_string(mapping: dict[str, Any], key: str, label: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label}.{key} must be a non-empty string")
    return value.strip()


def _string_list(mapping: dict[str, Any], key: str, label: str) -> tuple[str, ...]:
    value = mapping.get(key)
    if isinstance(value, str) and value.strip():
        return (value.strip(),)
    if not isinstance(value, list) or not value or not all(isinstance(x, str) and x.strip() for x in value):
        raise ValueError(f"{label}.{key} must be a non-empty string or array of strings")
    return tuple(x.strip() for x in value)


def _localized(mapping: dict[str, Any], key: str, label: str) -> dict[str, str]:
    raw = _need_mapping(mapping.get(key), f"{label}.{key}")
    result = {
        lang: text.strip()
        for lang, text in raw.items()
        if isinstance(lang, str) and isinstance(text, str) and text.strip()
    }
    if "sk" not in result:
        raise ValueError(f"{label}.{key} must contain a non-empty 'sk' value")
    return result


def _render_localized(values: dict[str, str], context: dict[str, object]) -> dict[str, str]:
    rendered: dict[str, str] = {}
    for lang, template in values.items():
        try:
            rendered[lang] = template.format(**context)
        except KeyError as exc:
            raise ValueError(f"Unknown template placeholder {exc} in {lang!r} text: {template!r}") from exc
    return rendered


def _add_localized(graph: Graph, subject: URIRef, predicate: URIRef, values: dict[str, str]) -> None:
    for lang, text in sorted(values.items()):
        graph.add((subject, predicate, Literal(text, lang=lang)))


def _bind_prefixes(graph: Graph) -> None:
    graph.bind("dcat", DCAT)
    graph.bind("dct", DCTERMS)
    graph.bind("foaf", FOAF)
    graph.bind("vcard", VCARD)
    graph.bind("leg", LEG)
    graph.bind("xsd", XSD)


def _public_url(base_url: str, relative_path: PurePosixPath | Path) -> str:
    raw = relative_path.as_posix().lstrip("/")
    return f"{base_url.rstrip('/')}/{quote(raw, safe='/-._~')}"


def _output_path(out_dir: Path, relative_path: PurePosixPath) -> Path:
    root = out_dir.resolve()
    candidate = (root / relative_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Configured LKOD path escapes the output directory: {relative_path}") from exc
    return candidate


def _local_path_for_url(base_url: str, out_dir: Path, url: str) -> Path:
    prefix = base_url.rstrip("/") + "/"
    if not url.startswith(prefix):
        raise ValueError(f"URL is outside configured base_url: {url}")
    rel = unquote(url[len(prefix):])
    candidate = (out_dir / PurePosixPath(rel)).resolve()
    root = out_dir.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"URL maps outside output directory: {url}") from exc
    return candidate


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "datasets": {}}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Cannot read LKOD state file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"LKOD state file {path} must contain an object")
    value.setdefault("version", 1)
    value.setdefault("datasets", {})
    return value


def _write_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _fingerprint(out_dir: Path, files: Iterable[Path]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    root = out_dir.resolve()
    for path in sorted(files, key=lambda p: p.as_posix()):
        resolved = path.resolve()
        rel = resolved.relative_to(root).as_posix()
        stat = resolved.stat()
        result.append({"path": rel, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    return result


def _job_output_is_current(job: ExportJob) -> bool:
    """Check that the main file belongs to the range currently planned for this job.

    Existence alone is not enough for current-year outputs: after the date range advances,
    a failed export could otherwise leave last month's file in place and the catalogue
    would silently describe it as the newer range.
    """
    if not job.out_path.is_file():
        return False

    meta = load_meta(job.meta_path)
    if not isinstance(meta, dict):
        return False

    expected = {
        "datasetName": job.dataset,
        "dateFrom": job.d_from.isoformat(),
        "dateTo": job.d_to.isoformat(),
        "fileFormat": job.fmt,
        "mergeStrategy": job.merge_strategy,
        "window": {"mode": job.window.mode, "size": job.window.size},
    }
    return all(meta.get(key) == value for key, value in expected.items())


def _candidate_outputs(job: ExportJob) -> tuple[Path, ...]:
    if not _job_output_is_current(job):
        return ()

    paths: list[Path] = [job.out_path]
    for step in job.postprocess:
        if step == "xlsx" and job.out_path.suffix.lower() == ".csv":
            candidate = job.out_path.with_suffix(".xlsx")
        elif step == "jsonld" and job.out_path.suffix.lower() == ".xml":
            candidate = job.out_path.with_suffix(".jsonld")
        else:
            continue
        if candidate.is_file():
            paths.append(candidate)

    # Preserve one output per actual path even if config repeats a postprocess step.
    return tuple(dict.fromkeys(paths))


def _discover_existing_jobs(config: AppConfig, out_dir: Path) -> list[ExportJob]:
    """Recover historical exports from their authoritative sidecar metadata.

    Some schedules intentionally fetch only the latest closed period (for example,
    the previous year). Older year folders must nevertheless remain catalogued.
    The `.meta.json` files contain the exact dataset, range and format that produced
    each main output, so they are safer to use than guessing from file names.
    """
    root = out_dir.resolve()
    jobs: list[ExportJob] = []
    for meta_path in root.rglob("*.meta.json"):
        try:
            meta_path.resolve().relative_to(root)
        except ValueError:
            continue

        meta = load_meta(meta_path)
        if not isinstance(meta, dict):
            continue
        dataset_name = meta.get("datasetName")
        fmt = meta.get("fileFormat")
        if not isinstance(dataset_name, str) or not isinstance(fmt, str):
            continue

        dataset = config.datasets.get(dataset_name)
        if dataset is None:
            continue
        format_spec = dataset.formats.get(fmt)
        if format_spec is None or not format_spec.enabled:
            continue

        window = meta.get("window")
        if not isinstance(window, dict):
            continue
        try:
            d_from = date.fromisoformat(str(meta["dateFrom"]))
            d_to = date.fromisoformat(str(meta["dateTo"]))
            window_mode = str(window["mode"])
            window_size = int(window["size"])
            merge_strategy = str(meta["mergeStrategy"])
        except (KeyError, TypeError, ValueError):
            continue

        suffix = ".meta.json"
        meta_text = str(meta_path)
        if not meta_text.endswith(suffix):
            continue
        out_path = Path(meta_text[: -len(suffix)])
        if out_path.suffix.lower() != f".{fmt.lower()}":
            continue

        jobs.append(
            ExportJob(
                dataset=dataset_name,
                fmt=fmt,
                d_from=d_from,
                d_to=d_to,
                out_path=out_path,
                meta_path=meta_path,
                window=WindowSpec(mode=window_mode, size=window_size),  # type: ignore[arg-type]
                merge_strategy=merge_strategy,  # type: ignore[arg-type]
                postprocess=format_spec.postprocess,
                keep_chunks=format_spec.keep_chunks,
                touch_mtime_to_range_end=False,
            )
        )
    return jobs


def _group_jobs(jobs: Iterable[ExportJob], out_dir: Path) -> dict[RecordKey, list[ExportJob]]:
    root = out_dir.resolve()
    grouped: dict[RecordKey, list[ExportJob]] = {}
    for job in jobs:
        parent = job.out_path.parent.resolve().relative_to(root)
        key = RecordKey(
            dataset=job.dataset,
            d_from=job.d_from,
            d_to=job.d_to,
            data_parent=PurePosixPath(parent.as_posix()),
        )
        grouped.setdefault(key, []).append(job)
    return grouped


def _effective_dataset_metadata(
    metadata_config: dict[str, Any], dataset_name: str
) -> dict[str, Any]:
    defaults = _need_mapping(metadata_config.get("dataset_defaults", {}), "dataset_defaults")
    datasets = _need_mapping(metadata_config.get("datasets", {}), "datasets")
    specific = _need_mapping(datasets.get(dataset_name), f"datasets.{dataset_name}")

    merged = dict(defaults)
    for key, value in specific.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def _add_contact(graph: Graph, owner: URIRef, contact_iri: URIRef, contact: dict[str, Any]) -> None:
    graph.add((owner, DCAT.contactPoint, contact_iri))
    contact_type = contact.get("type", "Organization")
    if contact_type not in {"Organization", "Individual"}:
        raise ValueError("contact.type must be 'Organization' or 'Individual'")
    graph.add((contact_iri, RDF.type, VCARD[contact_type]))
    _add_localized(graph, contact_iri, VCARD.fn, _localized(contact, "name", "contact"))
    email = _need_string(contact, "email", "contact")
    if not email.startswith("mailto:"):
        email = "mailto:" + email
    graph.add((contact_iri, VCARD.hasEmail, URIRef(email)))


def _add_terms_of_use(
    graph: Graph,
    distribution_iri: URIRef,
    terms_iri: URIRef,
    terms: dict[str, Any],
) -> None:
    graph.add((distribution_iri, LEG.termsOfUse, terms_iri))
    graph.add((terms_iri, RDF.type, LEG.TermsOfUse))
    fields = {
        "authors_work_type": LEG.authorsWorkType,
        "original_database_type": LEG.originalDatabaseType,
        "database_special_rights_type": LEG.databaseProtectedBySpecialRightsType,
        "personal_data_containment_type": LEG.personalDataContainmentType,
    }
    for config_key, predicate in fields.items():
        graph.add((terms_iri, predicate, URIRef(_need_string(terms, config_key, "terms_of_use"))))


def _record_context(key: RecordKey) -> dict[str, object]:
    return {
        "dataset": key.dataset,
        "year": key.d_from.year,
        "date_from": key.d_from.isoformat(),
        "date_to": key.d_to.isoformat(),
    }


def _serialize_turtle(graph: Graph) -> str:
    value = graph.serialize(format="turtle")
    text = value.decode("utf-8") if isinstance(value, bytes) else value
    return "# Generated by ces-export lcod_catalogue. Do not edit manually.\n\n" + text


def _build_record_graph(
    *,
    key: RecordKey,
    dataset_iri: URIRef,
    files: tuple[Path, ...],
    out_dir: Path,
    base_url: str,
    metadata: dict[str, Any],
    catalogue_contact: dict[str, Any],
    format_config: dict[str, Any],
    issued: str,
    modified: str,
) -> Graph:
    graph = Graph()
    _bind_prefixes(graph)
    context = _record_context(key)

    title = _render_localized(_localized(metadata, "title", f"datasets.{key.dataset}"), context)
    description = _render_localized(
        _localized(metadata, "description", f"datasets.{key.dataset}"), context
    )

    graph.add((dataset_iri, RDF.type, DCAT.Dataset))
    _add_localized(graph, dataset_iri, DCTERMS.title, title)
    _add_localized(graph, dataset_iri, DCTERMS.description, description)
    graph.add((dataset_iri, DCTERMS.publisher, URIRef(_need_string(metadata, "publisher", "dataset"))))
    graph.add((dataset_iri, DCTERMS.issued, Literal(issued, datatype=XSD.dateTime)))
    graph.add((dataset_iri, DCTERMS.modified, Literal(modified, datatype=XSD.dateTime)))

    for theme in _string_list(metadata, "themes", "dataset"):
        graph.add((dataset_iri, DCAT.theme, URIRef(theme)))
    graph.add(
        (
            dataset_iri,
            DCTERMS.accrualPeriodicity,
            URIRef(_need_string(metadata, "frequency", "dataset")),
        )
    )

    keywords = _need_mapping(metadata.get("keywords"), "dataset.keywords")
    keyword_count = 0
    for lang, words in sorted(keywords.items()):
        if isinstance(words, str):
            words = [words]
        if not isinstance(words, list):
            raise ValueError(f"dataset.keywords.{lang} must be a string or array")
        for word in words:
            if not isinstance(word, str) or not word.strip():
                raise ValueError(f"dataset.keywords.{lang} contains an empty/non-string value")
            graph.add((dataset_iri, DCAT.keyword, Literal(word.strip(), lang=lang)))
            keyword_count += 1
    if keyword_count == 0:
        raise ValueError(f"Dataset {key.dataset} must have at least one keyword")

    if metadata.get("dataset_type"):
        graph.add((dataset_iri, DCTERMS.type, URIRef(str(metadata["dataset_type"]))))
    for spatial in _string_list(metadata, "spatial", "dataset"):
        graph.add((dataset_iri, DCTERMS.spatial, URIRef(spatial)))

    temporal_iri = URIRef(str(dataset_iri) + "#temporal")
    graph.add((dataset_iri, DCTERMS.temporal, temporal_iri))
    graph.add((temporal_iri, RDF.type, DCTERMS.PeriodOfTime))
    graph.add((temporal_iri, DCAT.startDate, Literal(key.d_from.isoformat(), datatype=XSD.date)))
    graph.add((temporal_iri, DCAT.endDate, Literal(key.d_to.isoformat(), datatype=XSD.date)))

    contact = _need_mapping(metadata.get("contact", catalogue_contact), "dataset.contact")
    _add_contact(
        graph,
        dataset_iri,
        URIRef(str(dataset_iri) + "#contact-point"),
        contact,
    )

    terms = _need_mapping(metadata.get("terms_of_use"), "dataset.terms_of_use")
    root = out_dir.resolve()
    for file_path in sorted(files, key=lambda p: p.suffix.lower()):
        suffix = file_path.suffix.lower()
        fmt = _need_mapping(format_config.get(suffix), f"formats.{suffix}")
        format_code = _need_string(fmt, "format", f"formats.{suffix}")
        media_type = _need_string(fmt, "media_type", f"formats.{suffix}")
        labels = _localized(fmt, "title", f"formats.{suffix}")

        rel = file_path.resolve().relative_to(root)
        download_url = URIRef(_public_url(base_url, rel))
        token = suffix.removeprefix(".").replace(".", "-")
        distribution_iri = URIRef(str(dataset_iri) + f"#distribution-{token}")
        terms_iri = URIRef(str(distribution_iri) + "-terms-of-use")

        graph.add((dataset_iri, DCAT.distribution, distribution_iri))
        graph.add((distribution_iri, RDF.type, DCAT.Distribution))
        graph.add((distribution_iri, DCAT.accessURL, download_url))
        graph.add((distribution_iri, DCAT.downloadURL, download_url))
        graph.add((distribution_iri, DCTERMS.format, URIRef(format_code)))
        graph.add((distribution_iri, DCAT.mediaType, URIRef(media_type)))
        _add_localized(graph, distribution_iri, DCTERMS.title, labels)
        _add_terms_of_use(graph, distribution_iri, terms_iri, terms)

    return graph


def _build_catalogue_graph(
    *,
    catalogue_iri: URIRef,
    dataset_iris: Iterable[URIRef],
    catalogue: dict[str, Any],
) -> Graph:
    graph = Graph()
    _bind_prefixes(graph)
    graph.add((catalogue_iri, RDF.type, DCAT.Catalog))
    _add_localized(graph, catalogue_iri, DCTERMS.title, _localized(catalogue, "title", "catalogue"))
    _add_localized(
        graph,
        catalogue_iri,
        DCTERMS.description,
        _localized(catalogue, "description", "catalogue"),
    )
    graph.add((catalogue_iri, DCTERMS.publisher, URIRef(_need_string(catalogue, "publisher", "catalogue"))))
    if catalogue.get("homepage"):
        graph.add((catalogue_iri, FOAF.homepage, URIRef(str(catalogue["homepage"]))))
    contact = _need_mapping(catalogue.get("contact"), "catalogue.contact")
    _add_contact(
        graph,
        catalogue_iri,
        URIRef(str(catalogue_iri) + "#contact-point"),
        contact,
    )
    for dataset_iri in sorted(dataset_iris, key=str):
        graph.add((catalogue_iri, DCAT.dataset, dataset_iri))
    return graph


def _require(graph: Graph, subject: URIRef, predicate: URIRef, label: str) -> tuple[object, ...]:
    values = tuple(graph.objects(subject, predicate))
    if not values:
        raise ValueError(f"Validation failed: {label} is missing on {subject}")
    return values


def _validate_record(record: GeneratedRecord, *, base_url: str, out_dir: Path) -> None:
    graph = Graph().parse(record.record_path, format="turtle")
    dataset = record.dataset_iri
    if (dataset, RDF.type, DCAT.Dataset) not in graph:
        raise ValueError(f"Validation failed: {dataset} is not a dcat:Dataset")

    for predicate, label in (
        (DCTERMS.title, "dct:title"),
        (DCTERMS.description, "dct:description"),
        (DCTERMS.publisher, "dct:publisher"),
        (DCTERMS.issued, "dct:issued"),
        (DCTERMS.modified, "dct:modified"),
        (DCAT.theme, "dcat:theme"),
        (DCTERMS.accrualPeriodicity, "dct:accrualPeriodicity"),
        (DCAT.keyword, "dcat:keyword"),
        (DCAT.distribution, "dcat:distribution"),
    ):
        _require(graph, dataset, predicate, label)

    distributions = _require(graph, dataset, DCAT.distribution, "dcat:distribution")
    for distribution in distributions:
        if not isinstance(distribution, URIRef):
            raise ValueError(f"Validation failed: distribution is not identified by an IRI: {distribution}")
        if (distribution, RDF.type, DCAT.Distribution) not in graph:
            raise ValueError(f"Validation failed: {distribution} is not a dcat:Distribution")
        for predicate, label in (
            (LEG.termsOfUse, "leg:termsOfUse"),
            (DCAT.accessURL, "dcat:accessURL"),
            (DCAT.downloadURL, "dcat:downloadURL"),
            (DCTERMS.format, "dct:format"),
            (DCAT.mediaType, "dcat:mediaType"),
        ):
            _require(graph, distribution, predicate, label)

        for download_url in graph.objects(distribution, DCAT.downloadURL):
            if not isinstance(download_url, URIRef):
                raise ValueError(f"Validation failed: download URL is not an IRI: {download_url}")
            local_path = _local_path_for_url(base_url, out_dir, str(download_url))
            if not local_path.is_file():
                raise ValueError(
                    f"Validation failed: distribution URL has no local file: {download_url} -> {local_path}"
                )

        for terms in graph.objects(distribution, LEG.termsOfUse):
            if (terms, RDF.type, LEG.TermsOfUse) not in graph:
                raise ValueError(f"Validation failed: {terms} is not leg:TermsOfUse")
            for predicate in (
                LEG.authorsWorkType,
                LEG.originalDatabaseType,
                LEG.databaseProtectedBySpecialRightsType,
                LEG.personalDataContainmentType,
            ):
                _require(graph, terms, predicate, str(predicate))


def _validate_catalogue(
    catalogue_path: Path,
    catalogue_iri: URIRef,
    records: tuple[GeneratedRecord, ...],
    *,
    base_url: str,
    out_dir: Path,
) -> None:
    graph = Graph().parse(catalogue_path, format="turtle")
    if (catalogue_iri, RDF.type, DCAT.Catalog) not in graph:
        raise ValueError(f"Validation failed: {catalogue_iri} is not a dcat:Catalog")
    for predicate, label in (
        (DCTERMS.title, "dct:title"),
        (DCTERMS.description, "dct:description"),
        (DCTERMS.publisher, "dct:publisher"),
        (DCAT.dataset, "dcat:dataset"),
    ):
        _require(graph, catalogue_iri, predicate, label)

    expected = {record.dataset_iri for record in records}
    actual = set(graph.objects(catalogue_iri, DCAT.dataset))
    if actual != expected:
        raise ValueError(
            "Validation failed: catalogue dataset links differ from generated records: "
            f"missing={sorted(map(str, expected - actual))}, extra={sorted(map(str, actual - expected))}"
        )

    for dataset_iri in actual:
        if not isinstance(dataset_iri, URIRef):
            raise ValueError(f"Validation failed: catalogue dataset is not identified by an IRI: {dataset_iri}")
        record_path = _local_path_for_url(base_url, out_dir, str(dataset_iri))
        if not record_path.is_file():
            raise ValueError(f"Validation failed: catalogue points to missing record: {record_path}")

    for record in records:
        _validate_record(record, base_url=base_url, out_dir=out_dir)


def generate_catalogue(
    *,
    dataset_config_path: Path,
    metadata_config_path: Path,
    out_dir: Path,
    today: date,
    allow_missing: bool,
    start_year_override: int | None = None,
    end_year_override: int | None = None,
) -> tuple[Path, tuple[GeneratedRecord, ...]]:
    metadata_config = _need_mapping(
        json.loads(metadata_config_path.read_text(encoding="utf-8")),
        "metadata config",
    )
    base_url = _need_string(metadata_config, "base_url", "metadata config").rstrip("/")
    catalogue = _need_mapping(metadata_config.get("catalogue"), "catalogue")
    formats = _need_mapping(metadata_config.get("formats"), "formats")

    dataset_config = load_config(dataset_config_path)
    dataset_config = replace(
        dataset_config,
        defaults=replace(dataset_config.defaults, out_dir=out_dir),
    )
    planned_jobs = build_jobs(
        dataset_config,
        today=today,
        start_year_override=start_year_override,
        end_year_override=end_year_override,
    )
    # Historical sidecars add periods no longer emitted by a "latest period" schedule.
    # Planned jobs win for identical output paths so a stale current-period sidecar
    # cannot masquerade as the newly planned range.
    jobs_by_path = {
        job.out_path.resolve(): job for job in _discover_existing_jobs(dataset_config, out_dir)
    }
    jobs_by_path.update({job.out_path.resolve(): job for job in planned_jobs})
    jobs = list(jobs_by_path.values())
    grouped = _group_jobs(jobs, out_dir)

    catalogue_rel = PurePosixPath(_need_string(metadata_config, "catalogue_path", "metadata config"))
    records_dir = PurePosixPath(
        _need_string(metadata_config, "dataset_records_dir", "metadata config")
    )
    state_path_value = metadata_config.get("state_path", "lkod/.state.json")
    if not isinstance(state_path_value, str) or not state_path_value.strip():
        raise ValueError("metadata config.state_path must be a non-empty string")
    state_rel = PurePosixPath(state_path_value.strip())
    catalogue_path = _output_path(out_dir, catalogue_rel)
    state_path = _output_path(out_dir, state_rel)
    state = _load_state(state_path)
    state_datasets = _need_mapping(state.get("datasets", {}), "state.datasets")
    now = datetime.now().astimezone().isoformat(timespec="seconds")

    planned_record_keys = set(_group_jobs(planned_jobs, out_dir))
    records: list[GeneratedRecord] = []
    record_payloads: dict[Path, str] = {}
    new_state_datasets: dict[str, Any] = {}
    missing: list[str] = []

    for key, key_jobs in sorted(
        grouped.items(),
        key=lambda item: (
            item[0].data_parent.as_posix(),
            item[0].dataset,
            item[0].d_from,
            item[0].d_to,
        ),
    ):
        metadata = _effective_dataset_metadata(metadata_config, key.dataset)
        if metadata.get("enabled", True) is False:
            continue

        slug = _need_string(metadata, "slug", f"datasets.{key.dataset}")
        record_rel = records_dir / key.data_parent / f"{slug}.ttl"
        dataset_iri = URIRef(_public_url(base_url, record_rel))
        record_path = _output_path(out_dir, record_rel)

        distribution_files: list[Path] = []
        for job in key_jobs:
            distribution_files.extend(_candidate_outputs(job))
        distribution_files = list(dict.fromkeys(distribution_files))

        supported_files: list[Path] = []

        for path in distribution_files:
            suffix = path.suffix.lower()

            if suffix not in formats:
                raise ValueError(
                    f"No LKOD format mapping for generated file {path} "
                    f"(suffix {suffix!r})"
                )

            format_metadata = _need_mapping(
                formats.get(suffix),
                f"formats.{suffix}",
            )

            if format_metadata.get("enabled", True) is False:
                continue

            supported_files.append(path)

        if not supported_files:
            if key in planned_record_keys:
                missing.append(
                    f"{key.dataset} {key.d_from.isoformat()}..{key.d_to.isoformat()} ({key.data_parent})"
                )
            continue

        fingerprint = _fingerprint(out_dir, supported_files)
        old = state_datasets.get(str(dataset_iri), {})
        old = old if isinstance(old, dict) else {}
        issued = str(metadata.get("issued") or old.get("issued") or now)
        modified = str(old.get("modified") or now) if old.get("fingerprint") == fingerprint else now

        graph = _build_record_graph(
            key=key,
            dataset_iri=dataset_iri,
            files=tuple(supported_files),
            out_dir=out_dir,
            base_url=base_url,
            metadata=metadata,
            catalogue_contact=_need_mapping(catalogue.get("contact"), "catalogue.contact"),
            format_config=formats,
            issued=issued,
            modified=modified,
        )
        record_payloads[record_path] = _serialize_turtle(graph)
        records.append(
            GeneratedRecord(
                key=key,
                dataset_iri=dataset_iri,
                record_path=record_path,
                distribution_paths=tuple(supported_files),
            )
        )
        new_state_datasets[str(dataset_iri)] = {
            "issued": issued,
            "modified": modified,
            "fingerprint": fingerprint,
        }

    if missing and not allow_missing:
        formatted = "\n  - ".join(missing)
        raise RuntimeError(
            "Cannot build a complete LKOD catalogue because these planned records have no current "
            f"distribution:\n  - {formatted}\n"
            "Run the CES export successfully first, or use --allow-missing for a partial local test."
        )
    if not records:
        raise RuntimeError("No LKOD dataset records could be generated")

    # Do not touch the last known-good catalogue if any currently planned record
    # is incomplete. Once completeness is established, write records first and
    # the catalogue index last so it never points at a not-yet-created document.
    for record_path, payload in sorted(record_payloads.items(), key=lambda item: item[0].as_posix()):
        atomic_write_text(record_path, payload)
    for record in records:
        _validate_record(record, base_url=base_url, out_dir=out_dir)

    catalogue_iri = URIRef(_public_url(base_url, catalogue_rel))
    catalogue_graph = _build_catalogue_graph(
        catalogue_iri=catalogue_iri,
        dataset_iris=(record.dataset_iri for record in records),
        catalogue=catalogue,
    )
    # Write the index last, after all records exist.
    atomic_write_text(catalogue_path, _serialize_turtle(catalogue_graph))

    _validate_catalogue(
        catalogue_path,
        catalogue_iri,
        tuple(records),
        base_url=base_url,
        out_dir=out_dir,
    )

    state["version"] = 1
    state["datasets"] = new_state_datasets
    _write_state(state_path, state)

    if missing:
        print("[lkod] skipped records with no complete distribution:")
        for item in missing:
            print("  -", item)
    print(f"[lkod] catalogue: {catalogue_path}")
    print(f"[lkod] dataset records: {len(records)}")
    print(f"[lkod] distributions: {sum(len(r.distribution_paths) for r in records)}")
    print("[lkod] local RDF/link validation: OK")
    return catalogue_path, tuple(records)


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir
    if out_dir is None:
        import os

        env_out = os.environ.get("CES_EXPORT_OUT_DIR")
        if env_out:
            out_dir = Path(env_out)
    if out_dir is None:
        configured = load_config(args.config).defaults.out_dir
        if configured is not None:
            out_dir = configured
    if out_dir is None:
        raise SystemExit("No output directory. Pass --out-dir, set CES_EXPORT_OUT_DIR, or configure defaults.out_dir.")

    today = date.fromisoformat(args.today) if args.today else date.today()
    try:
        generate_catalogue(
            dataset_config_path=args.config,
            metadata_config_path=args.metadata_config,
            out_dir=out_dir.resolve(),
            today=today,
            allow_missing=args.allow_missing,
            start_year_override=args.start_year,
            end_year_override=args.end_year,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        import sys

        print(f"[lkod] ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
