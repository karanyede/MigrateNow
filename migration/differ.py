"""
Diff engine — computes the minimal INSERT / UPDATE change-set.

Given the set of source records and the set of records already present
in the target table, the differ categorises every source row as either:

* **insert** – record does not exist in target → needs POST
* **update** – record exists in target AND mapped fields have changed → PATCH
* **skipped** – record exists but no mapped fields have changed

Matching strategy:
  - **Coalesce mode** (preferred): A coalesce field on the target (e.g.
    ``u_legacy_sysid``) stores the source ``sys_id``. Matching is done
    against those values.
  - **sys_id mode** (fallback): Source and target share the same
    ``sys_id`` values.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("sn_migration")

# ServiceNow system fields that are auto-populated by the target
# instance (current user, current timestamp, etc.).  These will
# ALWAYS differ cross-instance, so comparing them would cause every
# record to be falsely marked as "changed".
SKIP_DIFF_FIELDS: set[str] = {
    "sys_id",
    "sys_created_by",
    "sys_created_on",
    "sys_updated_by",
    "sys_updated_on",
    "sys_mod_count",
    "sys_tags",
    # Salesforce system fields (auto-populated, always differ)
    "Id",
    "CreatedDate",
    "CreatedById",
    "LastModifiedDate",
    "LastModifiedById",
    "SystemModstamp",
    "IsDeleted",
}


@dataclass
class DiffResult:
    """Container for the diff output."""

    inserts: list[dict] = field(default_factory=list)
    updates: list[dict] = field(default_factory=list)
    skipped: int = 0

    @property
    def total(self) -> int:
        return len(self.inserts) + len(self.updates) + self.skipped


class TargetMatcher:
    """
    Builds O(1) indexes on target records based on coalesce configuration
    to support matching via primary key or multi-field AND/OR coalesce conditions.
    """

    def __init__(
        self,
        target_records: list[dict],
        field_mapping: dict[str, str],
        coalesce_config: dict | None = None,
    ) -> None:
        self.mapping = field_mapping
        self.reverse_mapping = {tgt: src for src, tgt in field_mapping.items()}
        self.coalesce_config = coalesce_config or {}
        
        self.enabled = self.coalesce_config.get("enabled", False) and bool(self.coalesce_config.get("fields"))
        self.logic = self.coalesce_config.get("logic", "AND").upper()
        self.fields = self.coalesce_config.get("fields", [])
        
        self.and_index = {}
        self.or_indexes = {}
        self.pk_index = {}
        
        if self.enabled:
            if self.logic == "AND":
                for rec in target_records:
                    key = tuple(str(rec.get(f, "")).strip() for f in self.fields)
                    if any(key):
                        self.and_index[key] = rec
            else:
                for f in self.fields:
                    self.or_indexes[f] = {}
                for rec in target_records:
                    for f in self.fields:
                        val = str(rec.get(f, "")).strip()
                        if val:
                            self.or_indexes[f][val] = rec
        else:
            for rec in target_records:
                key = rec.get("sys_id", "") or rec.get("Id", "")
                if key:
                    self.pk_index[key] = rec

    def match(self, src_record: dict) -> dict | None:
        if not self.enabled:
            sid = src_record.get("sys_id", "") or src_record.get("Id", "")
            return self.pk_index.get(sid)
            
        if self.logic == "AND":
            key_parts = []
            for f in self.fields:
                src_f = self.reverse_mapping.get(f)
                val = str(src_record.get(src_f, "")).strip() if src_f else ""
                key_parts.append(val)
            return self.and_index.get(tuple(key_parts))
        else:
            for f in self.fields:
                src_f = self.reverse_mapping.get(f)
                if src_f:
                    val = str(src_record.get(src_f, "")).strip()
                    if val and val in self.or_indexes[f]:
                        return self.or_indexes[f][val]
        return None


class DiffEngine:
    """
    Categorise source records into inserts vs. updates vs. skipped.

    Parameters
    ----------
    field_mapping : dict[str, str]
        ``{source_field: target_field}`` mapping.
    target_matcher : TargetMatcher
        An initialized TargetMatcher helper.
    coalesce_mode : bool
        If True, inject ``_target_sys_id`` into update records.
    """

    def __init__(
        self,
        field_mapping: dict[str, str],
        target_matcher: TargetMatcher,
        coalesce_mode: bool = False,
    ) -> None:
        self.mapping = field_mapping
        self.target_matcher = target_matcher
        self.coalesce_mode = coalesce_mode
        self._diff_log_count = 0

    def _is_different(self, source_rec: dict, target_rec: dict) -> bool:
        """Return True if any mapped fields differ between source and target."""
        for src_f, tgt_f in self.mapping.items():
            if src_f in SKIP_DIFF_FIELDS or tgt_f in SKIP_DIFF_FIELDS:
                continue

            src_val = str(source_rec.get(src_f, "")).strip()
            tgt_val = str(target_rec.get(tgt_f, "")).strip()

            if src_val != tgt_val:
                if self._diff_log_count < 3:
                    self._diff_log_count += 1
                    logger.info(
                        "DIFF mismatch #%d: %s→%s  src=%r  tgt=%r",
                        self._diff_log_count,
                        src_f, tgt_f,
                        src_val[:100], tgt_val[:100],
                    )
                return True

        return False

    def compute(self, source_records: list[dict]) -> DiffResult:
        result = DiffResult()
        
        effective_mapping = [
            (src, tgt) for src, tgt in self.mapping.items()
            if src not in SKIP_DIFF_FIELDS and tgt not in SKIP_DIFF_FIELDS
        ]

        for record in source_records:
            target_rec = self.target_matcher.match(record)

            if target_rec:
                is_diff = False
                for src_f, tgt_f in effective_mapping:
                    if str(record.get(src_f, "")).strip() != str(target_rec.get(tgt_f, "")).strip():
                        is_diff = True
                        break
                
                if is_diff:
                    if self.coalesce_mode:
                        record["_target_sys_id"] = target_rec.get("sys_id", "")
                        record["_target_sf_id"] = target_rec.get("Id", "")
                    result.updates.append(record)
                else:
                    result.skipped += 1
            else:
                result.inserts.append(record)

        logger.info(
            "Diff complete: %d inserts, %d updates, %d skipped (total %d).",
            len(result.inserts),
            len(result.updates),
            result.skipped,
            result.total,
        )
        return result
