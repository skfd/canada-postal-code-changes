# Background & Sources

Reference notes for the Canadian postal-codes project: what we're building, what
we're deliberately *not* building, and an annotated list of every source found
while researching how Canada Post postal codes are created, changed, and retired.

Consult this when writing site copy, framing the project's value, or deciding
whether a given piece of domain knowledge is already covered elsewhere.

---

## Project brief

### What we're building

A **longitudinal, visual, public-facing record of how the Canadian postal-code
map actually churns over time.** We take Statistics Canada's NAR snapshots
(2022, 2023, 2024-06, 2024-12, 2025-07, 2025-12) plus secondary sources, diff
them pair-by-pair, and surface the change:

- Codes **added** (new developments coming online).
- Codes **removed / retired**.
- **City / CSD changes** — sub-classified into meaningful vs. noise (encoding,
  accent, punctuation, spacing, abbreviation, boundary, rename, substantive).
- **Location shifts** where coordinates exist.
- Maps and FSA-level history so a non-expert can *see* the map changing.

The differentiator: we turn institutional, GIS-analyst knowledge into a story a
general reader can follow — **the change process, made visible over time.**

### What we're NOT building

- **Not another origin-story explainer.** The 1971 history is thoroughly covered
  (Wikipedia et al.). We link to it; we don't re-tell it.
- **Not a data-for-sale product.** The commercial vendors already do that. We're
  not selling datasets or address-validation APIs.
- **Not a live address lookup / validation / autocomplete tool.**
- **Not authoritative geocoding.** Pre-2026 snapshots largely lack coordinates;
  we don't pretend to precise point locations we don't have.
- **Not real-time.** We work from discrete NAR snapshots, not a live feed.
- **Not global.** Canada Post only. Other national systems are out of scope.

### The gap we fill

The *history* of postal codes is well told for general audiences. The *ongoing
change mechanics* (urbanization, retirement, reassignment, year-over-year churn)
exist only as (a) dry institutional rules in StatCan/Canada Post PDFs, or
(b) marketing bullets from data vendors ("3,445 created, 778 retired last year").
**No one has made that visual, longitudinal, and accessible.** That's our lane.

---

## Project findings & spikes

Our own analyses built on the sources below (in [`research/`](research/)):

- [`findings-reintroduced-codes.md`](research/findings-reintroduced-codes.md) —
  **verified against the DB:** 10,316 NAR codes (~1.2%) are retired then
  reintroduced; ~half of all `removed` events reverse; `rebuild_summary` flattens
  the gap. **Acted on 2026-07-21:** `temporary` / `reintroduced` subtypes are now
  labelled in the change log, the site shows when a removed code came back, and
  return figures are measured on NAR only (the merged union's 98.6% is a GeoNames
  coverage artifact). `rebuild_summary` gap-flagging remains deferred.
- [`canada-post-changes-bulletin.md`](research/canada-post-changes-bulletin.md) —
  **feasibility spike:** the Canada Post National Presortation Schematic is public
  but FSA-level PDFs for bulk mailers, not a per-code change feed → **no-go**; stay
  on NAR snapshot-diffing. PCCF (access-gated) is the real authoritative option.

---

## Sources

Grouped by type. Each entry: what it is, what it contains, and an italic
*Read if…* pointer for future reference.

**Deep-dive files.** Substantial sources have their own file in
[`research/`](research/) with a full review — linked inline below. Verification
legend: **[browser]** = page read in full via browser; **[PDF]** = PDF extracted;
**[snippet]** = described from search results / prior knowledge, *verify before
quoting in published copy*. **Entries with no tag are [snippet]** — the substantial
sources were prioritized for full review; the lighter/overlapping ones weren't.

### A. General-audience history & explainers

**[Wikipedia — Postal codes in Canada](https://en.wikipedia.org/wiki/Postal_codes_in_Canada)** · **[browser]** · deep dive: [`research/wikipedia-postal-codes-canada.md`](research/wikipedia-postal-codes-canada.md)
The single best digestible reference. Covers the 1925 Toronto numbered zones, the
flawed "Toronto 3, Ontario" city-zone system, the 1969 announcement by
Communications Minister Eric Kierans, the 1971–74 rollout (Ottawa trial → Manitoba
→ national, only 38.2% uptake by end-1974), CUPW's "Boycott the Postal Code" Day
(20 Mar 1975), plus full FSA/LDU structure, the address space math (7.2M
theoretical / ~830K active), urbanization, retirement, and the special codes
(H0H 0H0 Santa, military transition codes). Reference-style and static.
*Read if you need the canonical origin story or a reliable definition of any structural term — link here instead of re-explaining history.*

**[Placevy — The Late Development of Canadian Postal Codes](https://www.placevy.com/blog/the-late-development-of-canadian-postal-codes)**
A genuine general-audience history article: mail-volume explosion (staff grew
30,000 → 44,000 between 1957 and 1966) → automation pressure → 1971. Accessible,
no jargon. **Stops at initial implementation** — nearly nothing on how codes
change or retire afterward.
*Read if you want a model for accessible, narrative tone — but not for change mechanics.*

**[CBC archives (cbc.ca/1.2459693)](https://www.cbc.ca/1.2459693)**
CBC archival coverage tied to the introduction of the postal-code system. Useful
period colour and public-reaction framing.
*Read if you want contemporary reaction / archival media framing of the 1970s rollout.*

**[PostGrid CA — What is a Postal Code and its Significance](https://www.postgrid.ca/what-is-postal-code/)**
Retail/marketing explainer. Clear on format (ANA NAN), FSA/LDU split, and the
urbanization example (rural G0N → urban G3N). Light, sales-adjacent.
*Read if you want a plain-language definition of urbanization with a worked example.*

**[ZIP-Codes.com — Canadian Postal Codes: Complete Guide](https://www.zip-codes.com/canadian/canadian-postal-codes.asp)**
Vendor guide covering structure, FSA/LDU, province-letter allocation. Overlaps
Wikipedia at a lighter level.
*Read if you want a compact structural cheat-sheet.*

**[Academic Kids — Canadian postal code](https://academickids.com/encyclopedia/index.php/Canadian_postal_code)**
An old mirror of an early Wikipedia article. Historical value only; likely stale.
*Read if cross-checking how the public explanation was framed years ago — otherwise skip for Wikipedia.*

**[Quora — What is the history of Canadian postal codes?](https://www.quora.com/What-is-the-history-of-Canadian-postal-codes)**
Crowd answers; anecdotal, unverified.
*Read if hunting for anecdote or lived-experience framing — verify anything before using.*

### B. Institutional & technical (authoritative on change mechanics)

**[StatCan — How Postal Codes Map to Geographic Areas (Working Paper, PDF)](https://www150.statcan.gc.ca/n1/pub/92f0138m/92f0138m2007001-eng.pdf)** · **[PDF]** · deep dive: [`research/statcan-how-postal-codes-map-to-geography.md`](research/statcan-how-postal-codes-map-to-geography.md)
The rigorous academic argument for why a postal code is *not* a clean geographic
unit: the many-to-many problem, "illusory precision," and temporal instability
(codes retire/reallocate/create, so historical codes don't map to current
geography). The intellectual backbone for our signal-vs-noise thesis.
*Read if you need to explain, rigorously, what a postal-code change represents geographically — the strongest source to mine for site copy (read the actual PDF before quoting).*

**[StatCan — PCCF Technical Specifications, Section 4](https://www150.statcan.gc.ca/n1/pub/92-153-g/2011002/tech-eng.htm)** · **[browser]** · deep dive: [`research/statcan-pccf-technical-specs.md`](research/statcan-pccf-technical-specs.md)
The record layout that defines what a "change" *is*: **birth date**, **retired
date** (with the key caveat that codes are *reintroduced*), single-link indicator,
representative points, AAA→CCC quality indicators, delivery mode types, the
"Retired 2005" file, and the full census-geography linkage. Maps almost 1:1 onto
our differ's concepts.
*Read if you need the precise definitions StatCan uses for created / retired / effective-dated codes — the most directly useful source for grounding our differ.*

**[StatCan PCCF, November 2000 (U of T mirror, PDF)](https://mdl.library.utoronto.ca/sites/default/public/mdldata/open/canada/national/statcan/postalcodes/pccf/1996/2000nov/pccf_nov00.pdf)**
A concrete historical PCCF release. Shows the file format and the state of the
code base circa 2000.
*Read if you want a historical baseline / an example of raw PCCF structure.*

**[Canada Post — Addressing guidelines: Postal codes](https://www.canadapost-postescanada.ca/cpc/en/support/articles/addressing-guidelines/postal-codes.page)** · **[browser]**
The authority's own definition — but **thin**: just confirms the `ANA NAN` form and
the FSA (major area) / LDU (smallest delivery unit) split. Citable, not deep.
*Read if you need the official, citable format definition straight from Canada Post — nothing more.*

**[Canada Post — FSA list (Feb 2026, PDF)](https://www.canadapost-postescanada.ca/cpc/doc/en/support/kb/nps/NONLETTERMAIL_FSA_LIST_FEB_2026.pdf)**
Current authoritative list of all Forward Sortation Areas.
*Read if you need a ground-truth roster of valid FSAs to validate our FSA-level views.*

**[ISED — Forward Sortation Area, Definition](https://ised-isde.canada.ca/site/office-superintendent-bankruptcy/en/statistics-and-research/forward-sortation-area-fsa-and-north-american-industry-classification-naics-reports/forward-sortation-area-definition)** · **[browser]**
Clean government definition of the FSA with the full first-char → province/region
table (A=NL … Y=Yukon, X=NWT+NU, ON split K/L/M/N/P, QC split G/H/J). Concise;
overlaps the Wikipedia/PCCF structure sections.
*Read if you want a short, citable, non-Canada-Post government definition of "FSA" plus the province-letter table.*

**[StatCan Forward Sortation Areas (FSA) — ArcGIS overview](https://www.arcgis.com/home/item.html?id=ada17f8f54244b688e3ef4dda17a0849)**
StatCan FSA boundary layer hosted on ArcGIS.
*Read if we ever add authoritative FSA boundary polygons to the maps.*

**[U of T Map & Data Library — Forward Sortation Areas](https://mdl.library.utoronto.ca/collections/geospatial-data/forward-sortation-areas)**
Academic library collection page for FSA geospatial data.
*Read if sourcing FSA boundary files with clear provenance/licensing.*

### C. Commercial data vendors (the "marketing-bullet" group)

These sell datasets/APIs. They publish change statistics purely as sales copy —
useful to cite for scale, not as a model, and evidence of the gap we fill.

**[GeoPostcodes — Canada Zip Code dataset](https://www.geopostcodes.com/country/canada/zip-code/)**
Commercial dataset with monthly updates. Source of stats like "453 records added
May 1, 2026; 926,425 unique postal codes."
*Read if you want vendor-published churn numbers to cite for scale/context.*

**[Geocoder.ca — free data](http://www.geocoder.ca/?freedata=1)**
Crowdsourced Canadian postal data — one of our secondary sources.
*Read if working on the Geocoder.ca ingestion path or comparing against NAR.*

**[Geografx — Postal Code FSA/LDU data](https://www.geografx.com/postal-code-fsaldu-fsa-canada)**
Commercial FSA/LDU mapping datasets.
*Read if evaluating commercial boundary/LDU data — reference only, we're not buying.*

**[Spotzi — Forward Sortation Areas (PC3) dataset](https://www.spotzi.com/en/data-catalog/datasets/forward-sortation-areas-(pc3)/)**
FSA boundary dataset vendor.
*Read if comparing FSA boundary sources.*

**[datahub.io — Postal Codes CA](https://datahub.io/logistics/postal-codes-ca)**
Open packaged Canadian postal-code dataset.
*Read if you want a quick open dataset for spot-checks.*

**[Ontario Open Data — Canada postal code data](https://data.ontario.ca/dataset/canada-postal-code-data)**
Government open-data listing.
*Read if you want an openly-licensed reference set.*

**[Frank da Cruz — Canadian Postal Codes (Columbia)](http://ftp.columbia.edu/~fdc/postal/postal-ca.html)**
Long-standing hobbyist reference page on postal formats worldwide, Canada section.
*Read if you want a concise, no-nonsense technical description of the format.*

**[postalcodes-ca (PyPI)](https://pypi.org/project/postalcodes-ca/)**
Python library for Canadian postal-code lookup/validation.
*Read if we want an off-the-shelf validation/lookup lib rather than rolling our own.*

**[djbelieny/geoinfo-dataset (GitHub)](https://github.com/djbelieny/geoinfo-dataset)**
Combined US/Canada zip/postal + city/state/lat-long CSV dataset.
*Read if you need a quick lat/long enrichment source for spot-checks.*

**[OpenStreetMap talk-ca — Postcodes in Canada thread](https://lists.openstreetmap.org/pipermail/talk-ca/2019-October/009458.html)**
Community discussion of postcode data/licensing issues in Canada.
*Read if you hit licensing/data-availability questions — good on the legal grey areas.*

---

## One-line takeaways

- **History is a solved problem** for general readers → link, don't re-tell.
- **Change mechanics are documented only institutionally** → StatCan working paper
  ("How Postal Codes Map to Geography") + PCCF specs are the mining grounds.
- **Vendors have the numbers, not the story** → cite for scale, undercut on narrative.
- **No visual, longitudinal, public explainer of the churn exists** → that's us.
