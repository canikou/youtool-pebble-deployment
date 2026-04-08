from __future__ import annotations

from yt_assist.storage.database import SanitizePreview, SanitizeReport


def sanitize_preview_has_actions(preview: SanitizePreview) -> bool:
    return any(
        value > 0
        for value in (
            preview.canonical_rename_candidates,
            preview.non_active_proof_paths_prunable,
            preview.orphaned_proof_files,
            preview.stale_import_files,
        )
    )


def sanitize_confirmation_message(preview: SanitizePreview) -> str:
    lines = ["Review this sanitize pass before continuing:"]
    lines.append(f"Receipts checked: {preview.total_receipts_checked}")
    lines.append(f"Referenced proof paths: {preview.referenced_proof_paths}")
    lines.append(f"Canonical proof renames: {preview.canonical_rename_candidates}")
    lines.append(f"Rename collisions left untouched: {preview.rename_collisions}")
    lines.append(f"Paid/invalidated proof paths safe to trim: {preview.non_active_proof_paths_prunable}")
    lines.append(
        "Paid/invalidated proof paths retained without source URLs: "
        f"{preview.non_active_proof_paths_retained}"
    )
    lines.append(f"Orphaned proof files: {preview.orphaned_proof_files}")
    lines.append(f"Stale upload-import files: {preview.stale_import_files}")
    lines.append(
        f"Duplicate proof groups by hash: {preview.duplicate_proof_groups} "
        f"({preview.duplicate_proof_files} extra file copies)"
    )
    if preview.sample_renames:
        lines.append(f"Rename sample: {', '.join(preview.sample_renames)}")
    if preview.sample_prunable_receipts:
        lines.append(
            "Prunable receipt sample: "
            + ", ".join(f"`{receipt_id}`" for receipt_id in preview.sample_prunable_receipts)
        )
    if preview.sample_retained_receipts:
        lines.append(
            "Retained receipt sample: "
            + ", ".join(f"`{receipt_id}`" for receipt_id in preview.sample_retained_receipts)
        )
    if preview.sample_orphan_files:
        lines.append(f"Orphan file sample: {', '.join(f'`{name}`' for name in preview.sample_orphan_files)}")
    if preview.sample_stale_import_files:
        lines.append(
            "Stale import sample: "
            + ", ".join(f"`{name}`" for name in preview.sample_stale_import_files)
        )
    if preview.sample_duplicate_groups:
        lines.append(f"Duplicate hash sample: {'; '.join(preview.sample_duplicate_groups)}")
    lines.append("A backup export will be saved before cleanup is applied.")
    lines.append("Press Apply Safe Cleanup to continue, or Cancel to abort.")
    return "\n".join(lines)


def sanitize_noop_message(preview: SanitizePreview) -> str:
    return (
        "Nothing currently needs the safe cleanup pass.\n"
        f"Receipts checked: {preview.total_receipts_checked}\n"
        f"Retained non-active proof paths without source URLs: {preview.non_active_proof_paths_retained}\n"
        f"Duplicate proof groups by hash still detected: {preview.duplicate_proof_groups}\n"
        "No proof-path normalizations, safe proof trims, orphaned files, or stale import files were found."
    )


def sanitize_completed_message(
    actor_user_id: int,
    backup_file_name: str,
    report: SanitizeReport,
) -> str:
    preview = report.preview
    lines = [f"Sanitize complete for <@{actor_user_id}>."]
    lines.append(f"Backup export saved: `{backup_file_name}`")
    lines.append(f"Receipt proof paths updated: {report.receipt_paths_updated}")
    lines.append(f"Proof files renamed: {report.proof_files_renamed}")
    lines.append(f"Non-active proof files deleted: {report.proof_files_deleted}")
    lines.append(f"Retained non-active proof paths without source URLs: {preview.non_active_proof_paths_retained}")
    lines.append(f"Orphaned proof files deleted: {report.orphaned_files_deleted}")
    lines.append(f"Stale upload-import files deleted: {report.stale_import_files_deleted}")
    if preview.rename_collisions > 0:
        lines.append(f"Rename collisions left untouched: {preview.rename_collisions}")
    if preview.duplicate_proof_groups > 0:
        lines.append(
            f"Duplicate proof groups still detected by hash: {preview.duplicate_proof_groups} "
            f"({preview.duplicate_proof_files} extra file copies)"
        )
    return "\n".join(lines)
