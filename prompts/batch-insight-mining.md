# Batch insight-mining prompt

Run this after a new NAR snapshot has been downloaded, processed, diffed, and
classified (`postal-codes refresh --source nar && postal-codes classify`, so
`data/postal_codes.db` holds the new period). It asks an agent to **mine** the newest
batch and write the long-form, categorised, reasoned content that fills a snapshot's
own deep-dive page. This is a twice-a-year pop-science project: the goal is that a
reader *understands the release completely*, not that they see a status number. Depth,
grouping, real examples, and honest interpretation are the whole point.

Paste everything below the line into Claude Code from the repo root.

---

You are the data journalist and analyst for a Canadian postal-code change tracker. A
new Statistics Canada NAR snapshot is loaded into `data/postal_codes.db`. Produce the
full write-up for this snapshot's dedicated page: a long-form narrative, a breakdown of
every change group with its own categorisation and *interpreted reasoning*, real named
examples, and geographic analysis. Dig hard — this runs about twice a year.

## Data

SQLite `data/postal_codes.db`, source `merged` (see `src/db.py`):

- `postal_code_snapshots` — (postal_code, snapshot_date, source_type): `province_abbr,
  city_name, fsa, latitude, longitude, csd_code, address_count`. **Note: NAR snapshots
  before 2026 carry no coordinates; lat/long exist only from the 2026 GeoNames merge.**
- `postal_code_changes` — `change_type` (`added | removed | city_changed | csd_changed
  | location_shifted`), `change_subtype`, `snapshot_before`, `snapshot_after`,
  `old_value`, `new_value`, `province_abbr`, `fsa`.
- `postal_code_summary` — current state per code.

## The rule that governs everything: signal vs. noise

Most `city_changed` rows are **not real**. The feed re-encodes French/Acadian names
between releases, so accent repairs (`Main-Ã-Dieu → Main-A-Dieu`) register as changes.
**Real only if `change_subtype IN ('substantive','boundary','rename')`.** The rest
(`encoding`, `accent_normalization`, `punctuation`, `spacing`, `abbreviation`) is
technical noise — report it as its own group for transparency, never fold it into real
totals, never lead with it. `added`/`removed`/`csd_changed` are real events. Also look
at `csd_changed` (municipality/Census-Subdivision reassignment) — it is a strong
boundary/amalgamation signal and is easy to overlook.

## Steps

1. **Scope the batch**: newest `snapshot_after` and the transition from the prior
   snapshot. Headline metrics with your own SQL — added, removed, csd_changed, real
   city changes, and (separately) encoding noise. Verify net active delta == added −
   removed. Note the active count before → after.
2. **Break down each change group** with its own queries: per-province split, per-FSA
   concentration, and the subtype mix. Look for where the *raw* ranking and the *real*
   ranking disagree (they have before — QC dominates raw city changes but not always
   real ones).
3. **Pull real, named examples** for every group — actual `postal_code`, `old_value →
   new_value`, city, province, FSA. Prefer vivid, recognisable places. For additions,
   sample coordinates (2026 only) for the map.
4. **Interpret — the "reasoning" per group.** For each group write *why it likely
   happened*, grounded in the pattern you see (e.g. "a ring of settlements around the
   capital all relabelled to the city name → municipal amalgamation"; "zero removals +
   large step → bulk delivery-area load, not organic growth"; "encoding cluster is
   entirely accented-name provinces → feed re-encode"). **This is inference, not
   documented fact — label it as such** (`inferred · not official`). Where a StatCan or
   Canada Post methodology note actually supports a claim, cite it; otherwise say you're
   hypothesising. Never invent an official reason.
5. **Sanity-check** every number against SQL you ran. Flag anything that looks like a
   data artefact. If a batch is genuinely dull, say so plainly instead of inflating it.

## Deliverables

**A. `snapshot_<date>.json`** for the deep-dive page:

```json
{
  "snapshot": "2026-02-10",
  "range": "Dec 2025 – Feb 2026",
  "active_before": 853955, "active_after": 906382,
  "metrics": { "added": 52427, "removed": 0, "csd_changed": 0,
               "city_real": 8088, "city_noise": 75318 },
  "narrative": ["<paragraph>", "<paragraph>", "..."],
  "groups": [
    { "key": "boundary", "name": "Boundary reassignments", "count": 6392,
      "provinces": [["QC",2273],["BC",1987],["ON",1642],["NS",341],["AB",145]],
      "reasoning": ["<inferred paragraph>", "..."],
      "examples": [["C1E2V7","Brackley Beach","Charlottetown","PE"], "..."] }
  ],
  "map_points": [[lon,lat], "..."]
}
```

**B. Overview cards** — 3–4 short findings for the site's front page (see the front
page's `findings` shape), each a one-sentence, bolded, verified highlight.

**C. A human note** — anything worth a manual look next cycle, plus data-quality
concerns.

Voice: plain, curious, specific. One vivid verified example beats three vague claims.
All headline counts, splits, and examples must be exact from the DB; all "why" text
must be visibly marked as interpretation.
