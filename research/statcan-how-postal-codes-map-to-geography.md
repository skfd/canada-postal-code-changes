# StatCan Working Paper — How Postal Codes Map to Geographic Areas

- **URL:** https://www150.statcan.gc.ca/n1/pub/92f0138m/92f0138m2007001-eng.pdf
- **Catalogue:** 92F0138MIE2007001 (Geography Working Paper Series), 43 pp.
- **Type:** Academic / government working paper
- **Reviewed:** 2026-07-21 (PDF, extracted via WebFetch — read the PDF itself before
  quoting anything verbatim; this summary is a fast-model extraction, not a page read)

*Read if you want the rigorous, citable argument for WHY a postal code is not a
clean geographic unit — the intellectual backbone for our "signal vs. noise" thesis
and the honest caveats we should put on any map or aggregation.*

---

## The core argument

Postal codes exist for **mail-delivery efficiency**, census geography exists for
**stable statistical analysis** — different purposes, so they do not align. Treating
postal codes as clean geographic units introduces real, systematic error. The PCCF
exists to bridge them, but the bridge is an **approximation, not a definitive
assignment.**

## How postal geography diverges from census geography

- **Different logic:** codes follow mail routes/delivery patterns, not population or
  administrative boundaries.
- **Boundary misalignment:** postal-code areas rarely coincide with census areas.
- **Instability:** codes change/retire/reallocate on Canada Post's operational
  schedule, not the census cycle.

## The many-to-many problem (the paper's central technical point)

- One postal code can span **multiple** census divisions/subdivisions.
- One census area can contain parts of **multiple** postal codes.
- **Neither nests cleanly** inside the other → rollup/aggregation is inherently lossy.
- Assigning a spanning code to a single census unit creates **artificial precision.**

## PCCF geocoding & its error implications

- Links each code to census geography via coordinates (centroid / representative
  point). See the companion [PCCF technical specs](statcan-pccf-technical-specs.md)
  for the block-face / dissemination-block / dissemination-area representative-point
  machinery and the AAA→CCC quality indicators.
- **"Precision is illusory"** — codes look exact but mask geographic heterogeneity.
- The PCCF gives **best-guess linkages**, and **requires regular updates as codes
  change.**

## Postal codes over time (directly relevant to our diffing)

- Codes are **retired** (reduced volume, consolidation), **reallotted** to new
  areas as delivery needs shift, and **created** for growth/efficiency.
- Consequence, stated plainly: **historical postal-code data may not map to current
  geographic definitions** → longitudinal analysis is hard *exactly because* the
  code set churns. This is the analytical problem our snapshot-diffing makes visible.

## Caveats for analysts (worth echoing as honest disclaimers on our site)

1. Precision is illusory — heterogeneity hides inside a code.
2. Not hierarchical — codes don't roll up cleanly.
3. Operational, not statistical — boundaries reflect mail logistics.
4. Temporally unstable — definitions change; **PCCF version dates matter.**
5. PCCF is approximation, not truth.
6. Many-to-many complexity must be accounted for.

## How we use this

- It is the **best academic citation** for framing our project: we are literally
  visualizing the "temporal instability" this paper warns analysts about.
- Reinforces our **"not authoritative geocoding"** boundary — when we place map
  pins, we should borrow this paper's humility (representative points, not truth).
- Motivates our **classifier**: distinguishing a genuine Canada-Post change from
  census-boundary drift is the practical face of the many-to-many problem.
- Action item: this is the one source worth reading in the **original PDF** (saved
  to scratchpad) before we lift any wording into published copy.
