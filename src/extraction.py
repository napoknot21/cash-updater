from __future__ import annotations

import re
import polars as pl

from src.config import COUNTERPARTIES
from typing import Dict, List, Optional, Set, Tuple, Iterable


def _compile_subject_pattern (subject: Iterable[str] | str) -> str :
    """
    Return a case-insensitive regex string usable by Polars .str.contains().
    - If `subject` is a regex-looking string, use it as-is (prefix (?i) if missing).
    - Else treat as a list/semicolon-separated words and add word boundaries.
    """
    if isinstance(subject, str) :

        s = subject.strip()
        
        # looks like a regex if it contains any common meta
        if any(ch in s for ch in r".*+?[]()|{}^$\\"):
            pat = s or r"^(?!)"
        
        else :
        
            words = [w.strip() for w in s.split(";") if w.strip()]
            pat = "|".join(rf"\b{re.escape(w)}\b" for w in words) if words else r"^(?!)"
    
    else :

        words = [str(w).strip() for w in subject if str(w).strip()]
        pat = "|".join(rf"\b{re.escape(w)}\b" for w in words) if words else r"^(?!)"

    if not pat.startswith("(?i)") :
        pat = f"(?i){pat}"
    
    return pat


def _normalize_rules (rules : Optional[Dict[str, Dict]]) -> Dict[str, Dict] :
    """
    Normalize a rules dict:
      - emails/domains lowercased sets
      - derive domains from emails if missing
      - compile subject pattern to a case-insensitive regex string
      - filenames lowercased set
    """
    if not rules :
        return {}

    out: Dict[str, Dict] = {}

    for name, rule in rules.items() :

        emails = {str(e).strip().lower() for e in rule.get("emails", set()) if str(e).strip()}
        domains = {str(d).strip().lower() for d in rule.get("domains", set()) if str(d).strip()}
        
        if not domains :
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


def _extract_sender_columns(df: pl.DataFrame) -> pl.DataFrame :
    """
    Add helper columns:
      _from_lc, _sender_email, _sender_domain, _subject
    """
    email_rx = r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})"
    
    out = (

        df.with_row_index("_rid")
            
            .with_columns(

                pl.col("from").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_from_lc"),
                pl.col("subject").cast(pl.Utf8).fill_null("").alias("_subject"),
            
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


def _normalize_attachments(df : pl.DataFrame, attachment_column: Optional[str]) -> pl.DataFrame :
    """
    Create _files as a lowercased list[str] (empty if none).
    """
    if attachment_column and attachment_column in df.columns :

        return df.with_columns(

            pl.when(pl.col(attachment_column).is_null())
              .then(pl.lit([]))
              .when(pl.col(attachment_column).is_list())
              .then(pl.col(attachment_column).list.eval(pl.element().cast(pl.Utf8).str.to_lowercase()))
              .otherwise(pl.concat_list([pl.col(attachment_column).cast(pl.Utf8).str.to_lowercase().list()]))
              .alias("_files")
        
        )
    
    return df.with_columns(pl.lit([]).alias("_files"))



def _init_assignment(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add assignment columns: _assigned, _score.
    """
    return df.with_columns(

        pl.lit("UNMATCHED").alias("_assigned"),
        pl.lit(-1).alias("_score"),
    
    )



def _filter_attachments_only (df: pl.DataFrame) -> pl.DataFrame :
    """
    Keep rows where hasAttachments == True.
    - If 'hasAttachments' is missing, return empty (strict policy).
    - Accepts booleans or strings like 'true'/'1'/'yes'.
    """
    if df.is_empty() :
        return df
    
    if "hasAttachments" not in df.columns :
        return df.clear()

    cond = pl.coalesce(
    
        [
            pl.col("hasAttachments").cast(pl.Boolean, strict=False),
            pl.col("hasAttachments").cast(pl.Utf8, strict=False).str.to_lowercase().is_in(["true", "1", "yes", "y"]),
        ]
    
    ).fill_null(False)

    return df.filter(cond)



def _assign_by_emails(df: pl.DataFrame, name: str, emails: Set[str]) -> pl.DataFrame :
    """
    
    """
    if not emails :
        return df
    
    mask = (pl.col("_assigned") == "UNMATCHED") & pl.col("_sender_email").is_in(sorted(emails))
    
    return df.with_columns(
        pl.when(mask).then(pl.lit(name)).otherwise(pl.col("_assigned")).alias("_assigned"),
        pl.when(mask).then(pl.lit(100)).otherwise(pl.col("_score")).alias("_score"),
    )


def _assign_by_domains (df : pl.DataFrame, name : str, domains : Set[str]) -> pl.DataFrame :
    """
    
    """
    if not domains :
        return df
    
    mask = (pl.col("_assigned") == "UNMATCHED") & pl.col("_sender_domain").is_in(sorted(domains))
    
    return df.with_columns(
        pl.when(mask).then(pl.lit(name)).otherwise(pl.col("_assigned")).alias("_assigned"),
        pl.when(mask).then(pl.lit(80)).otherwise(pl.col("_score")).alias("_score"),
    )


def _assign_by_subject_and_filenames(
    df: pl.DataFrame,
    name: str,
    subject_re: str,
    filenames: Set[str],
) -> pl.DataFrame:
    subj_hit = pl.col("_subject").str.contains(subject_re, literal=False, strict=False)
    base = (pl.col("_assigned") == "UNMATCHED") & subj_hit

    if filenames:
        fn_any = pl.col("_files").list.eval(pl.element().is_in(sorted(filenames))).list.any()
        cond = base & fn_any
    else:
        # Allow subject-only if filenames set is empty
        cond = base

    return df.with_columns(
        pl.when(cond).then(pl.lit(name)).otherwise(pl.col("_assigned")).alias("_assigned"),
        pl.when(cond).then(pl.lit(50)).otherwise(pl.col("_score")).alias("_score"),
    )


def _apply_rule (df : pl.DataFrame, name : str, rule : Dict) -> pl.DataFrame :
    """
    Apply one counterparty rule with priority:
    emails ->domains -> subject -> optional filenames.
    """
    df = _assign_by_emails(df, name, rule["emails"])
    df = _assign_by_domains(df, name, rule["domains"])
    df = _assign_by_subject_and_filenames(df, name, rule["subject_re"], rule["filenames"])
    
    return df


def _materialize_buckets (dfw : pl.DataFrame, original : pl.DataFrame, names: List[str]) -> Dict[str, pl.DataFrame]:
    
    drops = ["_rid", "_from_lc", "_sender_email", "_sender_domain", "_subject", "_files", "_assigned", "_score"]
    out: Dict[str, pl.DataFrame] = {}
    
    for name in names + ["UNMATCHED"] :

        part = dfw.filter(pl.col("_assigned") == name).drop(drops, strict=False)
        
        out[name] = part.select([c for c in original.columns if c in part.columns] +
                                [c for c in part.columns if c not in original.columns])
    
    return out


def split_by_counterparty(
        
        df: pl.DataFrame,
        rules: Optional[Dict[str, Dict]] = None,
        attachment_column: Optional[str] = None,
    
    ) -> Dict[str, pl.DataFrame]:
    """
    Factorized + vectorized splitter.
    Keeps ONLY rows with hasAttachments == True.

    Priority per counterparty:
      (1) exact email  (score=100)
      (2) domain       (score=80)
      (3) subject + filename  (score=50)
          (subject-only allowed if that rule's filenames set is empty)
    """
    if df is None or df.is_empty():
        return {}
    
    rules = COUNTERPARTIES if rules is None else rules

    # Strict: attachments only
    df = _filter_attachments_only(df)

    if df.is_empty():
        # Nothing to classify; still return a single UNMATCHED bucket for consistency
        return {"UNMATCHED": df}

    nrules = _normalize_rules(rules)
    
    work = _extract_sender_columns(df)
    work = _normalize_attachments(work, attachment_column)
    work = _init_assignment(work)

    for name, rule in nrules.items() :
        work = _apply_rule(work, name, rule)

    return _materialize_buckets(work, df, list(nrules.keys()))
