from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .models import ExportedPaymentProof

MAX_LONG_EDGE = 2_000
MAX_PIXELS = 4_000_000
PNG_PASSTHROUGH_LIMIT_BYTES = 2_500_000
JPEG_QUALITY = 92
PROOF_VALUE_SEPARATOR = "\n"


@dataclass(slots=True)
class StoredProof:
    path: Path
    filename: str


def sanitize_file_name(file_name: str) -> str:
    return file_name.translate({ord(ch): "_" for ch in '\\/:*?"<>|'})


def join_proof_values(values: list[str]) -> str | None:
    cleaned = [value.strip() for value in values if value and value.strip()]
    return "\n".join(cleaned) if cleaned else None


def split_proof_values(values: str | None) -> list[str]:
    if values is None:
        return []
    return [line.strip() for line in values.splitlines() if line.strip()]


def proof_image_references(proof_path: str | None, fallback_url: str | None) -> list[str]:
    attachment_references: list[str] = []
    for path in split_proof_values(proof_path):
        name = Path(path).name
        if name:
            attachment_references.append(f"attachment://{name}")
    if attachment_references:
        return attachment_references
    return split_proof_values(fallback_url)


def proof_image_reference(proof_path: str | None, fallback_url: str | None) -> str | None:
    references = proof_image_references(proof_path, fallback_url)
    return references[0] if references else None


def first_proof_value(values: str | None) -> str | None:
    values_list = split_proof_values(values)
    return values_list[0] if values_list else None


def first_proof_file_name(proof_path: str | None) -> str | None:
    first = first_proof_value(proof_path)
    return Path(first).name if first else None


def first_proof_source_url(values: str | None) -> str | None:
    return first_proof_value(values)


def detect_content_type(extension: str | None) -> str | None:
    ext = (extension or "").lower()
    if ext in {"jpg", "jpeg"}:
        return "image/jpeg"
    if ext == "webp":
        return "image/webp"
    if ext == "png":
        return "image/png"
    return None


def file_name_from_url(url: str) -> str | None:
    parsed = urlparse(url.strip())
    last_segment = parsed.path.rsplit("/", 1)[-1]
    cleaned = last_segment.split("?", 1)[0].strip()
    return cleaned or None


def save_proof_attachment(
    attachment_dir: Path | str,
    receipt_id: str,
    file_name: str,
    bytes_data: bytes,
) -> StoredProof:
    attachment_dir = Path(attachment_dir)
    attachment_dir.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_file_name(file_name)
    stored_name = f"{receipt_id}-{safe_name}"
    path = attachment_dir / stored_name
    path.write_bytes(bytes_data)
    return StoredProof(path=path, filename=stored_name)


def load_embedded_payment_proofs(proof_path: str | None) -> list[ExportedPaymentProof]:
    proofs: list[ExportedPaymentProof] = []
    for path in split_proof_values(proof_path):
        file_path = Path(path)
        if not file_path.exists():
            continue
        proofs.append(
            ExportedPaymentProof(
                file_name=file_path.name or None,
                content_type=detect_content_type(file_path.suffix.lstrip(".")),
                data_base64=base64.b64encode(file_path.read_bytes()).decode("ascii"),
            )
        )
    return proofs


def load_embedded_payment_proof(proof_path: str | None) -> ExportedPaymentProof | None:
    proofs = load_embedded_payment_proofs(proof_path)
    return proofs[0] if proofs else None


def materialize_imported_payment_proofs(
    attachment_dir: Path | str,
    receipt_id: str,
    embedded: list[ExportedPaymentProof],
    existing_path: str | None = None,
    source_url: str | None = None,
) -> list[Path]:
    if embedded:
        paths: list[Path] = []
        for proof in embedded:
            bytes_data = base64.b64decode(proof.data_base64.encode("ascii"))
            file_name = proof.file_name
            if not file_name:
                file_name = file_name_from_url(first_proof_source_url(source_url) or "") or "proof.png"
            paths.append(save_proof_attachment(attachment_dir, receipt_id, file_name, bytes_data).path)
        return paths

    existing_paths = [Path(path) for path in split_proof_values(existing_path) if Path(path).exists()]
    if existing_paths:
        return existing_paths

    return []


def materialize_imported_payment_proof(
    attachment_dir: Path | str,
    receipt_id: str,
    embedded: ExportedPaymentProof | None,
    existing_path: str | None = None,
    source_url: str | None = None,
) -> Path | None:
    if embedded is not None:
        paths = materialize_imported_payment_proofs(
            attachment_dir,
            receipt_id,
            [embedded],
            existing_path,
            source_url,
        )
        return paths[0] if paths else None
    paths = materialize_imported_payment_proofs(attachment_dir, receipt_id, [], existing_path, source_url)
    return paths[0] if paths else None

