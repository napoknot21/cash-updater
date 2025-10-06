from __future__ import annotations

import re
import polars as pl
from typing import Dict, Iterable, Optional, Set

from src.config import COUNTERPARTIES


# ---------- helpers: subject + filenames normalization ----------

_RE_FWD_RX = re.compile(r"^(?:(?:re|fw|fwd)\s*:\s*)+", flags=re.IGNORECASE)

def _strip_re_fwd_prefixes(s: str) -> str:
    return _RE_FWD_RX.sub("", s or "")

def _normalize_subject_text_expr(col: pl.Expr) -> pl.Expr:
    """
    Lowercase, strip, collapse internal whitespace, and remove repeated Re:/Fw: prefixes.
    """
    return (
        col.cast(pl.Utf8)
           .fill_null("")
           .str.replace_all(r"^\s+", "")           # leading ws
           .str.replace_all(r"\s+$", "")           # trailing ws
           .str.replace_all(r"\s+", " ")
           .map_elements(_strip_re_fwd_prefixes, return_dtype=pl.Utf8)
           .str.to_lowercase()
           .str.replace_all(r"\s+", " ")
           .str.strip_chars()
    )

def _compile_subject_set(subject: Iterable[str] | str) -> Set[str]:
    """
    Build a set of *normalized* subject phrases for exact equality.
    Accepts single string or iterable. If single string contains ';', split on ';'.
    """
    def norm(s: str) -> str:
        s = _strip_re_fwd_prefixes((s or "").strip().lower())
        s = re.sub(r"\s+", " ", s)
        return s

    if isinstance(subject, str):
        vals = [v for v in (subject.split(";") if ";" in subject else [subject]) if v and v.strip()]
    else:
        vals = [str(v) for v in subject if str(v).strip()]

    out = {norm(v) for v in vals if norm(v)}
    return out


# ---------- rules normalization ----------

def _normalize_rules(rules: Optional[Dict[str, Dict]]) -> Dict[str, Dict]:
    """
    Normalize the raw config:
      - emails => lowercased set
      - subject => compiled exact-match *set* (normalized)
      - filenames => lowercased set
    """
    if not rules:
        return {}

    out: Dict[str, Dict] = {}
    for name, rule in rules.items():
        emails = {str(e).strip().lower() for e in rule.get("emails", set()) if str(e).strip()}
        subject_set = _compile_subject_set(rule.get("subject", []))
        filenames = {str(f).strip().lower() for f in rule.get("filenames", set()) if str(f).strip()}
        out[name] = {"emails": emails, "subject_set": subject_set, "filenames": filenames}
    return out


# ---------- dataframe preparation ----------

def _extract_sender_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds helper columns:
      _sender_email (lowercased exact email if found),
      _subject_norm (normalized for exact equality),
      _from_lc (lowercased raw 'from' or 'sender' for extraction),
    """
    email_rx = r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})"

    from_col = "from" if "from" in df.columns else ("sender" if "sender" in df.columns else None)
    if from_col is None:
        # Build an empty usable column to keep the pipeline vectorized
        df = df.with_columns(pl.lit("").alias("_from_lc"))
    else:
        df = df.with_columns(pl.col(from_col).cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_from_lc"))

    return (
        df.with_columns(_normalize_subject_text_expr(pl.col("subject")).alias("_subject_norm"))
          .with_columns(
              pl.when(pl.col("_from_lc").str.contains(email_rx))
                .then(pl.col("_from_lc").str.extract(email_rx, 1))
                .otherwise(pl.lit(""))
                .alias("_sender_email")
          )
    )


def _normalize_attachments(df: pl.DataFrame, attachment_column: Optional[str]) -> pl.DataFrame:
    """
    Create _files as a lowercased list[str] (empty if none/column missing).
    Works whether the column is already a list or a single string filename.
    """
    if not attachment_column or attachment_column not in df.columns:
        return df.with_columns(pl.lit([]).alias("_files"))

    col = pl.col(attachment_column)

    return df.with_columns(
        pl.when(col.is_null())
          .then(pl.lit([]))
          .when(col.is_list())
          .then(
              col.cast(pl.List(pl.Utf8))
                 .list.eval(pl.element().cast(pl.Utf8).str.to_lowercase())
          )
          .otherwise(
              # single filename -> wrap into list
              col.cast(pl.Utf8)
                 .str.to_lowercase()
                 .map_elements(lambda s: [s], return_dtype=pl.List(pl.Utf8))
          )
          .alias("_files")
    )


def _filter_attachments_only(df: pl.DataFrame) -> pl.DataFrame:
    """
    Keep only rows with hasAttachments == True (accepts bool or common truthy strings).
    If column missing => return empty (strict policy).
    """
    if df.is_empty():
        return df
    if "hasAttachments" not in df.columns:
        return df.clear()

    cond = pl.coalesce(
        [
            pl.col("hasAttachments").cast(pl.Boolean, strict=False),
            pl.col("hasAttachments")
              .cast(pl.Utf8, strict=False)
              .str.to_lowercase()
              .is_in(["true", "1", "yes", "y"]),
        ]
    ).fill_null(False)

    return df.filter(cond)


# ---------- strict bucket filter ----------

def _filter_bucket_strict(
    df_work: pl.DataFrame,
    original: pl.DataFrame,
    name: str,
    rule: Dict,
) -> pl.DataFrame:
    """
    Strict selection for one counterparty:
      - sender email ∈ rule['emails']
      - subject_norm ∈ rule['subject_set'] (empty set => matches nothing)
      - if filenames provided: require ≥1 filename match WHEN files exist in row
    """
    emails: Set[str] = rule["emails"]
    subject_set: Set[str] = rule["subject_set"]
    filenames: Set[str] = rule["filenames"]

    if not emails or not subject_set:
        return original.clear()  # strict: both must be configured

    mask_email = pl.col("_sender_email").is_in(sorted(emails))
    mask_subject = pl.col("_subject_norm").is_in(sorted(subject_set))

    if filenames:
        files_len = pl.col("_files").list.len()
        any_match = pl.col("_files").list.eval(
            pl.element().cast(pl.Utf8).is_in(sorted(filenames))
        ).list.any()
        mask_files = pl.when(files_len > 0).then(any_match).otherwise(pl.lit(True))
    else:
        mask_files = pl.lit(True)

    part = df_work.filter(mask_email & mask_subject & mask_files)

    helpers = {"_from_lc", "_sender_email", "_subject_norm", "_files"}
    if part.is_empty():
        return part.drop(list(helpers), strict=False)

    return part.select(
        [c for c in original.columns if c in part.columns]
        + [c for c in part.columns if c not in original.columns]
    ).drop(list(helpers), strict=False)


# ---------- public API ----------

def split_by_counterparty(
    df: pl.DataFrame,
    rules: Optional[Dict[str, Dict]] = None,
    attachment_column: Optional[str] = None,
) -> Dict[str, pl.DataFrame]:
    """
    STRICT splitter:
      1) Keep only emails with attachments.
      2) Exact sender email (lowercased) AND exact normalized subject (after stripping Re:/Fw:).
      3) Optional filename gating: if filenames configured, require ≥1 filename match only when
         the row has filenames; otherwise don't block.

    Returns: dict {counterparty: DataFrame} with ONLY matched buckets.
    """
    if df is None or df.is_empty():
        return {}

    df1 = _filter_attachments_only(df)
    if df1.is_empty():
        return {}

    nrules = _normalize_rules(COUNTERPARTIES if rules is None else rules)
    work = _extract_sender_columns(df1)
    work = _normalize_attachments(work, attachment_column)

    out: Dict[str, pl.DataFrame] = {}
    for name, rule in nrules.items():
        part = _filter_bucket_strict(work, df1, name, rule)
        if not part.is_empty():
            out[name] = part
    return out
