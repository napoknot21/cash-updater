from __future__ import annotations

import re
import polars as pl

from src.config import COUNTERPARTIES
from typing import Dict, List, Optional, Set, Tuple, Iterable


# -------------------- Subject pattern --------------------

def _compile_subject_pattern(subject: Iterable[str] | str) -> str:
    """
    Return a case-insensitive regex usable by Polars .str.contains().
    - If `subject` looks like a regex, use it as-is (prefix (?i) if missing).
    - Else treat as list/semicolon-separated words and add word boundaries for each word.
    """
    if isinstance(subject, str):
        s = subject.strip()
        # looks like a regex if it contains any common meta
        if any(ch in s for ch in r".*+?[]()|{}^$\\"):
            pat = s or r"^(?!)"
        else:
            words = [w.strip() for w in s.split(";") if w.strip()]
            pat = "|".join(rf"\b{re.escape(w)}\b" for w in words) if words else r"^(?!)"
    else:
        words = [str(w).strip() for w in subject if str(w).strip()]
        pat = "|".join(rf"\b{re.escape(w)}\b" for w in words) if words else r"^(?!)"

    if not pat.startswith("(?i)"):
        pat = f"(?i){pat}"
    return pat


# -------------------- Rules normalization --------------------

def _normalize_rules(rules: Optional[Dict[str, Dict]]) -> Dict[str, Dict]:
    """
    Normalize a rules dict:
      - emails/domains lowercased sets
      - derive domains from emails if missing
      - compile subject pattern to a case-insensitive regex string
      - filenames lowercased set
    """
    if not rules:
        return {}

    out: Dict[str, Dict] = {}

    for name, rule in rules.items():
        emails = {str(e).strip().lower() for e in rule.get("emails", set()) if str(e).strip()}
        domains = {str(d).strip().lower() for d in rule.get("domains", set()) if str(d).strip()}

        if not domains:
            domains = {e.split("@", 1)[-1] for e in emails if "@" in e}

        subj_pat = _compile_subject_pattern(rule.get("subject", []))
        filenames = {str(f).strip().lower() for f in rule.get("filenames", set()) if str(f).strip()}

        out[name] = {
            "emails": emails,
            "domains": domains,
            "subject_re": subj_pat,
            "filenames": filenames,
        }

    return out


# -------------------- DataFrame preparation --------------------

def _extract_sender_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add helper columns:
      _from_lc, _sender_email, _sender_domain, _subject (normalized spaces)
    """
    email_rx = r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})"

    out = (
        df.with_row_index("_rid")
        .with_columns(
            pl.col("From").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_from_lc"),
            # Normalize subject a bit for stable 'contains' (trim + collapse spaces)
            pl.col("Subject").cast(pl.Utf8).fill_null("")
              .str.strip_chars()
              .str.replace_all(r"\s+", " ")
              .alias("_subject"),
        )
        .with_columns(
            pl.when(pl.col("_from_lc").str.contains(email_rx))
              .then(pl.col("_from_lc").str.extract(email_rx, 1))
              .otherwise(pl.lit(""))
              .alias("_sender_email")
        )
        .with_columns(
            pl.when(pl.col("_sender_email").str.contains("@"))
              .then(pl.col("_sender_email").str.split_exact("@", 2).struct.field("field_1"))
              .otherwise(pl.lit(""))
              .alias("_sender_domain")
        )
    )

    return out


def _normalize_attachments(df: pl.DataFrame, attachment_column: Optional[str]) -> pl.DataFrame :
    """
    Create _files as a lowercased list[str] (empty if none).
    Works whether the column is already a list or a single string filename.
    """
    if attachment_column and attachment_column in df.columns :

        col = pl.col(attachment_column)
        
        return df.with_columns(
            
            pl.when(col.is_null())
                .then(pl.lit([]))
                .when(col.is_list())
                .then(

                    col.cast(pl.List(pl.Utf8)).list.eval(pl.element().cast(pl.Utf8).str.to_lowercase())
                )
                .otherwise(
                    
                    col.cast(pl.Utf8).str.to_lowercase().map_elements(lambda s: [s], return_dtype=pl.List(pl.Utf8))
                
                )
                .alias("_files")
            )
    
    return df.with_columns(pl.lit([]).alias("_files"))


def _init_assignment (df: pl.DataFrame) -> pl.DataFrame :
    """
    Add assignment columns: _assigned, _score.
    """
    return df.with_columns(
        
        pl.lit("UNMATCHED").alias("_assigned"),
        pl.lit(-1).alias("_score"),
    
    )


def _filter_attachments_only (df: pl.DataFrame, column : str = "Attachments") -> pl.DataFrame :
    """
    Keep rows where hasAttachments == True.
    - If 'hasAttachments' is missing, return empty (strict policy).
    - Accepts booleans or strings like 'true'/'1'/'yes'.
    """
    if df.is_empty() :
        return df

    if column not in df.columns :
        return df.clear()

    cond = pl.coalesce(
        [
            pl.col(column).cast(pl.Boolean, strict=False),
            pl.col(column).cast(pl.Utf8, strict=False).str.to_lowercase().is_in(["true", "1", "yes", "y"]),
        ]
    ).fill_null(False)

    return df.filter(cond)


# -------------------- Assignment (email/domain AND subject) --------------------

def _assign_by_emails (
        
        df: pl.DataFrame,
        name: str,
        emails: Set[str],
        subject_re: str,
        filenames: Set[str],
    
    ) -> pl.DataFrame :
    """
    Assign when sender email matches AND subject contains the configured pattern.
    If 'filenames' provided, require at least one filename match ONLY when the row has filenames.
    """
    if not emails or not subject_re or subject_re in (r"^(?!)", r"(?!)") :
        return df

    subj_hit = pl.col("_subject").str.contains(subject_re, literal=False, strict=False)
    base = (pl.col("_assigned") == "UNMATCHED") & pl.col("_sender_email").is_in(sorted(emails)) & subj_hit

    if filenames:
        files_len = pl.col("_files").list.len()
        fn_any = pl.col("_files").list.eval(
            pl.element().cast(pl.Utf8).str.to_lowercase().is_in(sorted(filenames))
        ).list.any()
        cond = base & pl.when(files_len > 0).then(fn_any).otherwise(pl.lit(True))
    else:
        cond = base

    return df.with_columns(
        pl.when(cond).then(pl.lit(name)).otherwise(pl.col("_assigned")).alias("_assigned"),
        pl.when(cond).then(pl.lit(100)).otherwise(pl.col("_score")).alias("_score"),
    )


def _assign_by_domains(
        
        df: pl.DataFrame,
        name: str,
        domains: Set[str],
        subject_re: str,
        filenames: Set[str],

    ) -> pl.DataFrame:
    """
    Assign when sender domain matches AND subject contains the configured pattern.
    If 'filenames' provided, require at least one filename match ONLY when the row has filenames.
    """
    if not domains or not subject_re or subject_re in (r"^(?!)", r"(?!)"):
        return df

    subj_hit = pl.col("_subject").str.contains(subject_re, literal=False, strict=False)
    base = (pl.col("_assigned") == "UNMATCHED") & pl.col("_sender_domain").is_in(sorted(domains)) & subj_hit

    if filenames:
        files_len = pl.col("_files").list.len()
        fn_any = pl.col("_files").list.eval(
            pl.element().cast(pl.Utf8).str.to_lowercase().is_in(sorted(filenames))
        ).list.any()
        cond = base & pl.when(files_len > 0).then(fn_any).otherwise(pl.lit(True))
    else:
        cond = base

    return df.with_columns(
        pl.when(cond).then(pl.lit(name)).otherwise(pl.col("_assigned")).alias("_assigned"),
        pl.when(cond).then(pl.lit(80)).otherwise(pl.col("_score")).alias("_score"),
    )


def _apply_rule (df: pl.DataFrame, name: str, rule: Dict) -> pl.DataFrame :
    """
    Require subject containment for both email- and domain-based assignment.
    """
    df = _assign_by_emails(df, name, rule["emails"], rule["subject_re"], rule["filenames"])
    df = _assign_by_domains(df, name, rule["domains"], rule["subject_re"], rule["filenames"])
    return df


# -------------------- Output buckets --------------------

def _materialize_buckets (dfw: pl.DataFrame, original: pl.DataFrame, names: List[str]) -> Dict[str, pl.DataFrame]:
    """
    Build the output dict of matched buckets + UNMATCHED.
    Keep original column order, append any new columns at the end (helpers dropped).
    """
    drops = ["_rid", "_from_lc", "_sender_email", "_sender_domain", "_subject", "_files", "_assigned", "_score"]
    out: Dict[str, pl.DataFrame] = {}

    for name in names + ["UNMATCHED"]:
        part = dfw.filter(pl.col("_assigned") == name).drop(drops, strict=False)
        out[name] = part.select(
            [c for c in original.columns if c in part.columns]
            + [c for c in part.columns if c not in original.columns]
        )

    return out


# -------------------- Public API --------------------

def split_by_counterparty (
        
        df: pl.DataFrame,
        rules: Optional[Dict[str, Dict]] = None,
        attachment_column: Optional[str] = None,
    
    ) -> Dict[str, pl.DataFrame] :
    """
    Vectorized splitter.
    Keeps ONLY rows with hasAttachments == True.

    Priority per counterparty (with *mandatory* subject containment):
      (1) exact email  (score=100)  AND subject contains pattern (+ optional filenames)
      (2) domain       (score=80)   AND subject contains pattern (+ optional filenames)

    Returns a dict with matched buckets + an "UNMATCHED" bucket.
    """
    rules = COUNTERPARTIES if rules is None else rules

    if df is None or df.is_empty() :
        return {}

    # Strict: attachments only
    df2 = _filter_attachments_only(df)

    if df2.is_empty() :
        # Nothing to classify; still return a single UNMATCHED bucket for consistency
        return {"UNMATCHED": df2}

    nrules = _normalize_rules(rules)

    work = _extract_sender_columns(df2)
    work = _normalize_attachments(work, attachment_column)
    work = _init_assignment(work)

    for name, rule in nrules.items() :
        work = _apply_rule(work, name, rule)

    return _materialize_buckets(work, df2, list(nrules.keys()))
