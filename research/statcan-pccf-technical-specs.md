# StatCan — PCCF Technical Specifications (Section 4)

- **URL:** https://www150.statcan.gc.ca/n1/pub/92-153-g/2011002/tech-eng.htm
- **Type:** Government technical reference (archived; PCCF May-2011 release)
- **Reviewed:** 2026-07-20 (full page read via browser)

*Read if you need the precise, authoritative definitions StatCan uses for
created / retired / effective-dated postal codes, or the exact record fields that
define what a "change" is — this is the single most useful source for grounding
our differ's semantics.*

---

## Why this matters most to us

This page defines the **record layout of the Postal Code Conversion File (PCCF)**
— the same kind of data our project diffs. Several fields map directly onto our
change-detection concepts and give us **authoritative vocabulary and edge cases**.

## The fields that define "change" (directly relevant to `differ.py`)

- **Birth date (`Birth_Date`)** — "the date when the postal code became
  effective." **All codes created before April 1983 carry `19830401`.** So a
  postal code has an official *creation* date. (Our "added" changes are, in PCCF
  terms, new birth dates.)
- **Retired date (`Ret_Date`)** — "the date when a postal code was retired."
  Pre-April-1983 retirements all carry `19830401`. **Active codes use the sentinel
  `19000001`.** Crucial caveat, verbatim: *"some postal codes have been retired
  and reintroduced at a later date."* → retirement is **not permanent**; a code
  can come back. Our diff must not assume removal is terminal.
- **Historic Delivery Mode Type (`H_DMT`)** — retains the *previous* delivery
  mode, i.e. the data model explicitly tracks state transitions.
- **Delivery Mode Type `Z`** = "Postal code is retired (no further delivery)."
  8,183 codes / 18,404 records were retired in this release.
- **Single Link Indicator (`SLI`)** — flags one canonical record per active code;
  *"every set of retired records for a postal code, for a given retirement date,
  has one SLI equal to '1'."* → the file keeps **multiple retirement generations**
  per code. This is the historical layering we're effectively reconstructing.

## Structure (authoritative version of the Wikipedia facts)

- Postal code = 6-char **`ANA NAN`** (e.g. K1A 0T6). 18 letters + 10 digits in use;
  **D, F, I, O, Q, U, W, Z not used.**
- **First char** allocated **east→west**; denotes a province/territory/major sector.
  Full official mapping (Table 4.2), notably ON split across **K, L, M, N, P** and
  QC across **G, H, J**; **X = NWT + Nunavut** together.
- **69 postal codes** in the PCCF link to a **different province** than their first
  letter implies — real cross-province anomalies (relevant to our province logic).
- **FSA** = first 3 chars, "well-defined and **stable** areas." As of May 2011:
  **1,638 FSAs** (1,454 urban + 184 rural). **Rural = 0 in 2nd position.**
- **LDU** = last 3 chars = a route: block-face, community mailbox (services both
  sides within a 200 m radius), apartment/business building, large-volume receiver,
  govt dept, delivery route, general delivery, or PO boxes.

## Geocoding & coordinates (why our pre-2026 snapshots lack coords)

The PCCF attaches **representative points**, not true addresses:
- **Rep_Pt_Type:** 1 = block-face (1.29M records), 2 = dissemination block (178K),
  3 = dissemination area (218K). A representative point "represents a line or a
  polygon" — centrally located or population-weighted.
- Coordinates come as **latitude/longitude in decimal degrees**, computed from a
  **Lambert conformal conic** projection.
- **Quality Indicator (`QI` = QI_1|QI_2|QI_3)**, AAA (best) → CCC (worst):
  QI_1 = certainty of correct census subdivision; QI_2 = correct street; QI_3 =
  correct address range. This is the **error model** for treating a postal code as
  a location — directly relevant to how confidently we can place pins on maps.

## Census-geography linkage (the analytic payoff, and the caveats)

Each postal code links to a stack of census geography, each with its own change
rules — the reason postal-geography is messy:
- **PR** (province), **CDuid** (census division), **CSDuid + CSDname + CSDtype**
  (census subdivision = municipality, name **as of Jan 1 2006**), CCScode, **SAC/
  SACtype** (metro/agglomeration influence zones), CTname (census tract — *splits*
  when population grows: 0042.00 → 0042.01/.02), ER, DPL, FED03uid (electoral
  district), **POP_CNTR_RA** (population centre — **its code is retired on
  amalgamation or falling below thresholds**), DAuid, dissemination block.
- Key point for us: **CSDname is frozen at a census reference date (2006 here).**
  When we see `city_changed`, part of what we're catching is exactly this kind of
  boundary/name reclassification vs. genuine Canada-Post renames — validating our
  classifier's 8 subtypes.

## Files & the "Retired 2005" concept

- A PCCF release = the PCCF + 3 name files + **`R2005.txt`** ("Retired 2005").
  **Codes retired before Jan 1 2006 live in the separate Retired file** to keep the
  main PCCF smaller → StatCan itself partitions history from current state, much
  like our snapshots vs. changes tables.
- Plain ASCII, fixed-width; bilingual file-naming keyed to the CPC reference date
  (e.g. `pccfNat_MAY11_fccpNat.zip`).

## Takeaways for our project

- Adopt PCCF's precise language: **birth date / retired date / reintroduced /
  single-link indicator / representative point / quality indicator.**
- **Retirement is reversible** — codes reappear. Our differ and any "removed"
  messaging must reflect that.
- Much apparent "city change" is **census-classification drift** (name frozen at a
  census date), not a Canada Post rename — supports our classifier's noise-vs-
  signal split.
- The **QI model** is the honest way to talk about how well a postal code maps to a
  point — useful when we caveat map pins.
- Pointer: this page explicitly refers deeper questions to the working paper
  *How Postal Codes Map to Geographic Areas* (Cat. 92F0138MIE2007001) — see
  [statcan-how-postal-codes-map-to-geography.md](statcan-how-postal-codes-map-to-geography.md).
