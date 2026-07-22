# Findings — Do postal codes get retired and reintroduced in our data?

- **Date:** 2026-07-21
- **Trigger:** The PCCF technical spec states *"some postal codes have been retired
  and reintroduced at a later date."* We diff snapshots pair-by-pair, so this asks
  whether our data actually contains present→absent→present codes, and how our
  model represents them. See [statcan-pccf-technical-specs.md](statcan-pccf-technical-specs.md).
- **Scope:** Investigation + report only (no code changes this pass).
- **Method:** Read-only SQL against `data/postal_codes.db`, two independent counts.

---

## Headline: YES — reintroduction is real and material

**10,316 NAR postal codes** appear, disappear, then reappear across the 7 NAR
snapshots. **Two independent methods agree exactly:**

1. **Ground truth (snapshots):** per-code presence vector across the 7 ordered
   dates; count codes whose presence has a `1…0…1` gap → **10,316**.
2. **Change-log cross-check (`postal_code_changes`):** codes with a `removed` event
   followed by a later `added` event → **10,316**.

Out of **867,425** distinct NAR codes, that's **~1.2%**.

## Context — this is genuine churn, not a snapshot artifact

NAR per-date distinct code counts:

| Snapshot | Distinct codes | Δ |
|---|---:|---:|
| 2022-01-01 | 823,658 | — |
| 2023-01-01 | 837,789 | +14,131 |
| 2024-06-01 | 853,835 | +16,046 |
| 2024-12-01 | 851,865 | −1,970 |
| 2025-07-01 | 852,091 | +226 |
| 2025-12-01 | 853,955 | +1,864 |
| 2026-06-01 | 856,929 | +2,974 |

The 2023 snapshot is **larger** than 2022, so the reappearances are **not** an
"undersized 2023 snapshot" artifact — codes genuinely drop out and return while the
overall set grows.

## The striking part — half of all "removals" reverse

NAR change-event counts:

| change_type | events |
|---|---:|
| csd_changed | 129,127 |
| city_changed | 80,569 |
| added | 54,107 |
| **removed** | **20,836** |

Of **20,836 `removed` events, 10,316 codes later reappear** — roughly **half of
everything we label "removed" is temporary.** Calling these "removed" overstates
permanence.

## How the current model represents it

**`differ.py`** (diffs consecutive pairs independently): a reappearance produces a
`removed` event in one pair and an unrelated `added` event in a later pair. **Nothing
links them**, and the reappearance is labeled a plain `added` (not `reintroduced`).

**`db.rebuild_summary`** collapses each code to `first_seen = MIN(date)` /
`last_seen = MAX(date)`, **silently spanning the gap.** Verified on samples:

| code | actual history | summary row |
|---|---|---|
| `A1A3V2` | present 2022, absent **2023 only**, back 2024-06+ | first=2022-01-01 last=2026-06-01 active=1 changes=2 |
| `A1B4M9` | present 2022, absent **2023 → 2025-12** (~3 yrs), back 2026-06 | first=2022-01-01 last=2026-06-01 active=1 changes=3 |

So the range spans one-snapshot blips (`1011111`) to multi-year absences
(`1000001`). In every case `postal_code_summary` shows the code as continuously
present 2022→2026. `is_active` is still **correct** (it reflects presence at the
latest date), and `total_changes` **does** count the churn — but `first_seen`/
`last_seen` give no hint of the absence.

## Caveats / honesty

- The two methods aren't fully independent — the change log is derived from the same
  snapshots by the differ — but their exact agreement confirms the differ is
  internally consistent on add/remove.
- We **cannot** distinguish true Canada Post retire-then-reissue from NAR
  **coverage/completeness variation** between snapshots without authoritative per-code
  dates. The PCCF's `Birth_Date`/`Ret_Date` fields would settle it — see the data-
  source note [[canada-post-changes-bulletin]] (PCCF is access-gated / previously
  skipped).

## Recommendation (for a future, separately-approved pass)

Document now; if we act later, cheapest-to-richest options:

1. **Terminology + docs caveat (lowest effort).** Stop implying `removed` is
   permanent; note on any change view that ~50% of removals reverse. No code change.
2. **Label reappearances in `differ.py`.** When an `added` code was previously
   `removed`, emit `change_type = 'reintroduced'` (or a subtype). Small, localized;
   requires re-running the diff. Makes the 10,316 visible as a distinct category.
3. **Gap-awareness in `rebuild_summary`.** Add e.g. a `had_gap` flag or an
   "appearances / span" so a reintroduced code isn't shown as continuously present.
4. **Authoritative fix (highest effort, gated).** Use PCCF `Birth_Date`/`Ret_Date`
   for true creation/retirement/reintroduction dates and sub-annual timing.

**Suggested if pursued:** (1) immediately (it's just wording/honesty), plus (2) as a
small, high-signal enhancement. Defer (3)/(4). No changes made pending user approval.

---

## Implemented 2026-07-21 — options (1) and (2)

- **`differ.label_reintroductions(source)`** — a pass over the whole change log for
  a source (not pair-by-pair), setting `change_subtype = 'temporary'` on removals
  that later reverse and `'reintroduced'` on the add that reverses them. Runs at the
  end of `diff_all_pairs`; also exposed as `postal-codes reintroductions --source X`
  so an existing DB can be labelled without re-diffing. Idempotent. Needed a new
  `idx_changes_pc` index on `postal_code_changes(postal_code)`.
- **NAR result:** 10,340 removal events labelled `temporary` (vs. 10,316 *distinct*
  codes counted above — a few codes are removed more than once).
- **Site:** `summary.json` gains `removed_returned`, `snapshots.json` gains a
  per-transition `removed_returned`, and `removed.json` rows gain `back` (the
  snapshot the code returned in). The Removed tab has a "Came back" column and a
  caveat; the hero and the per-snapshot "Removed" tile no longer imply retirement.
- **`api.py`:** `substantive_only` used to mean "subtype is NULL or real", which
  would now drop added/removed rows; it is scoped to `city_changed` instead.

### New caveat found while implementing — don't quote the merged number

Run on `source_type = 'merged'`, the pass reports **20,257 of 20,542 removals (98.6%)
as temporary**. That is an artifact, not a finding: merged snapshots are a *union* of
sources, and GeoNames (899,779 codes) first enters the union at **2026-02-10**,
"reviving" 12,610 codes that NAR had dropped. Coverage, not reissue.

So everything published about returns is measured on the **NAR series only**
(`static_generator._return_dates`), even though the rest of the site is generated
from `merged`. Per-transition NAR returns:

| transition | removed | later returned |
|---|---:|---:|
| 2022 → 2023 | 12,637 | 8,724 |
| 2023 → 2024-06 | 921 | 392 |
| 2024-06 → 2024-12 | 5,620 | 1,145 |
| 2024-12 → 2025-07 | 485 | 57 |
| 2025-07 → 2025-12 | 836 | 22 |
| 2025-12 → 2026-06 | 337 | 0 |

The decline down the column is **right-censored** — recent removals have had fewer
releases in which to return — so it is not evidence that removals became more
permanent. The site copy says so explicitly.
