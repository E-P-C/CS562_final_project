from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import re


@dataclass
class AggSpec:
    gv: str          # grouping variable id, e.g., "1"
    func: str        # sum/count/avg/min/max
    col: str         # column name or "*"
    alias: str       # internal/output field name


@dataclass
class PhiSpec:
    select_attrs: List[str]           # S:
    num_gv: int                       # n:
    grouping_attrs: List[str]         # V:
    aggs: List[AggSpec]               # F:
    predicates: Dict[str, str]        # sigma: gv -> predicate string (Python expression over env dict)
    having: List[List[str]]           # G: list of OR blocks, each a list of AND predicates


_FUNC_CANON = {"sum": "sum", "count": "count", "avg": "avg", "min": "min", "max": "max"}

# Accept tokens like: 1_sum_quant, 2_avg_price, 3_count_*
_AGG_RE = re.compile(r"^\s*(\d+)\s*_(sum|count|avg|min|max)\s*_\s*([A-Za-z_][A-Za-z0-9_]*|\*)\s*$", re.I)


def parse_phi_file(path: str) -> PhiSpec:
    """
    English comments:
    Tolerant parser for a common MF-structure "phi" input format.

    Expected sections:
      S: <select list>
      n: <num grouping variables>
      V: <grouping attributes>
      F: <aggregate list>
      sigma:
        <gv>: <predicate expression>
        ...

    Notes:
    - Separators can be commas or whitespace.
    - Predicates use Python boolean logic: and/or/not
    - Use single quotes for strings.
    - Predicates are evaluated over an environment dict containing:
        * row columns from sales
        * grouping attributes for the current MF entry
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Strip '#' comments and empty lines
    lines: List[str] = []
    for ln in raw.splitlines():
        ln = re.sub(r"#.*$", "", ln).strip()
        if ln:
            lines.append(ln)

    S: Optional[str] = None
    n: Optional[str] = None
    V: Optional[str] = None
    F: Optional[str] = None
    G: Optional[str] = None
    sigma_lines: List[str] = []

    mode: Optional[str] = None
    for ln in lines:
        lower = ln.lower()
        if lower.startswith("s:"):
            mode = "S"
            S = ln.split(":", 1)[1].strip()
            continue
        if lower.startswith("n:"):
            mode = "n"
            n = ln.split(":", 1)[1].strip()
            continue
        if lower.startswith("v:"):
            mode = "V"
            V = ln.split(":", 1)[1].strip()
            continue
        if lower.startswith("f:"):
            mode = "F"
            F = ln.split(":", 1)[1].strip()
            continue
        if lower.startswith("sigma:"):
            mode = "sigma"
            rest = ln.split(":", 1)[1].strip()
            if rest:
                sigma_lines.append(rest)
            continue
        if lower.startswith("g:"):
            mode = "G"
            G = ln.split(":", 1)[1].strip()
            continue

        # Continuation lines
        if mode == "sigma":
            sigma_lines.append(ln)
        elif mode == "S" and S is not None:
            S += " " + ln
        elif mode == "V" and V is not None:
            V += " " + ln
        elif mode == "F" and F is not None:
            F += " " + ln
        elif mode == "n" and n is not None:
            n += " " + ln

    if S is None or n is None or V is None:
        raise ValueError("phi input must include S:, n:, V: sections.")

    select_attrs = _split_list(S)
    num_gv = int(_first_token(n))
    grouping_attrs = _split_list(V)

    aggs: List[AggSpec] = []
    if F:
        for item in _split_list(F):
            m = _AGG_RE.match(item)
            if not m:
                raise ValueError(f"Invalid aggregate token in F: '{item}'. Expected like '1_sum_quant'.")
            gv = m.group(1)
            func = _FUNC_CANON[m.group(2).lower()]
            col = m.group(3)
            alias = f"{gv}_{func}_{col}"
            aggs.append(AggSpec(gv=gv, func=func, col=col, alias=alias))

    predicates: Dict[str, str] = {}
    for ln in sigma_lines:
        # Accept: "1: ..." or "1 -> ..." or "1 - ..."
        m = re.match(r"^\s*(\d+)\s*[:\- >]+\s*(.+)\s*$", ln)
        if m:
            gv = m.group(1)
            pred = m.group(2).strip()
            predicates[gv] = pred
        else:
            # If no gv is provided, append to gv=1 by AND
            if "1" not in predicates:
                predicates["1"] = ln.strip()
            else:
                predicates["1"] = f"({predicates['1']}) and ({ln.strip()})"

    if G:
        # Normalize spacing
        G = re.sub(r'\s+', ' ', G.strip())

        # Split OR (case-insensitive)
        or_blocks = re.split(r'\s+OR\s+', G, flags=re.IGNORECASE)

        having: List[List[str]] = []

        for block in or_blocks:
            and_parts = re.split(r'\s+AND\s+', block, flags=re.IGNORECASE)
            rewrite = []

            for cond in and_parts:
                cond = cond.strip()
                # rewrite aggregates
                cond = re.sub(
                    r"\b(\d+)\s*_(sum|count|avg|min|max)\s*_\s*([A-Za-z_][A-Za-z0-9_]*|\*)\b",
                    lambda m: f"entry['{m.group(0).strip()}']",
                    cond,
                )

                # 2) rewrite plain attributes (month, year, prod)
                cond = re.sub(
                    r"(?<!')\b[A-Za-z_][A-Za-z0-9_]*\b(?!')",
                    lambda m: m.group(0)
                    if m.group(0) in {"not", "True", "False", "entry"}
                    else f"entry['{m.group(0).strip()}']",
                    cond,
                )
                rewrite.append(cond)
            having.append(rewrite)

    return PhiSpec(
        select_attrs=select_attrs,
        num_gv=num_gv,
        grouping_attrs=grouping_attrs,
        aggs=aggs,
        predicates=predicates,
        having=having
    )


def _split_list(s: str) -> List[str]:
    parts: List[str] = []
    for chunk in s.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.extend(chunk.split())
    return [p.strip() for p in parts if p.strip()]


def _first_token(s: str) -> str:
    return s.strip().split()[0]

