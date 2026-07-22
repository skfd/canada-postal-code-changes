# Feasibility spike — Canada Post monthly changes bulletin as a data source

- **Reviewed:** 2026-07-21 (Canada Post NPS landing page inspected via browser;
  one restructure PDF fetched)
- **Question:** Is the Canada Post "National Presortation Schematic / monthly
  bulletin detailing postal code changes" (referenced by Wikipedia) a usable
  upstream feed of the added/retired/changed codes we currently reconstruct by
  diffing NAR snapshots?

*Read if you're considering a new data source for change events, or want the
verdict on why we stayed with NAR snapshot-diffing.*

---

## Verdict: NO-GO as a structured per-code change feed

The **National Presortation Schematic (NPS)** is real, **public, and free to
download** (no login on the landing page), updated **monthly** — but it is the
wrong shape for us. **Keep NAR snapshot-diffing as the backbone.**

## What the NPS actually is

- **URL (inspected):**
  https://www.canadapost-postescanada.ca/cpc/en/support/kb/business/mail-design-preparation/download-the-current-national-presortation-schematic.page
- Purpose: helps **bulk mailers presort** Publications Mail / Personalized Mail /
  Neighbourhood Mail for machine processing. It shows *how mail is distributed*, not
  a registry of postal-code births and deaths.
- Deliverables are **PDFs**, refreshed monthly, each with a validity window:
  **Schematic List**, **Amendment Letter**, **FSA List**, and a **"Planned
  Restructure for the next 6 months … (by FSA)"** PDF (example inspected:
  `/cpc/doc/en/support/kb/nps/restructures-from-du-2026-07-20-to-au-2026-11-16.pdf`).

## Why it doesn't fit

1. **Granularity is FSA (3-char), not postal code (6-char).** The restructure and
   FSA documents are organized *by FSA*. Our project tracks ~900K individual
   6-character codes; the NPS operates one level up. It cannot tell us which LDUs
   were added/retired.
2. **Format is PDF, not structured data.** Even the restructure PDF did not yield
   clean extractable text on fetch — parsing would be brittle, and it's presort
   tables, not change deltas.
3. **It's forward-looking presort ops, not a change ledger.** The "Amendment
   Letter" amends the *schematic*; it's about mail routing/restructuring for
   mailers, not an enumeration of code creations/retirements with effective dates.
4. **Licensing.** NPS is Canada Post proprietary content under its terms of use —
   not an open licence like our StatCan NAR (Statistics Canada Open Licence).
   Re-publishing parsed data could carry restrictions our current sources don't.

**Possible (low-value) use:** the by-FSA "planned restructure" PDFs could feed a
*qualitative* "upcoming FSA restructuring" preview, but the effort (PDF parsing,
proprietary content) outweighs the payoff.

## Better open alternatives (if we ever want authoritative change dates)

- **PCCF — Postal Code Conversion File** *(the right source, but access-gated).*
  Its per-code **`Birth_Date`** and **`Ret_Date`** fields are the authoritative
  creation/retirement dates — and it explicitly flags reintroduction and
  `DMT = Z` (retired). This would **directly answer the reintroduction question**
  (see [[findings-reintroduced-codes]]) and give exact sub-annual timing that
  snapshot-diffing can't. **But** PCCF is distributed via StatCan DLI / the
  **Community Data Program** (membership-gated; a Dec 2025 update exists there), and
  this project already recorded a **"No PCCF access — skipped"** decision. Revisit
  *only* if DLI/CDP access becomes available. See
  [statcan-pccf-technical-specs.md](statcan-pccf-technical-specs.md).
- **StatCan Open Database of Addresses (ODA)** — open-licensed, includes postal
  codes; useful for address enrichment, but it's addresses, not change deltas.
- **PCCF "Retired 2005" file** — open historical list of pre-2006 retirements;
  narrow and dated.

## Recommendation

- **Do not integrate the NPS.** Wrong granularity (FSA), wrong format (PDF), wrong
  purpose (presort ops), and proprietary licensing.
- **Stay on NAR snapshot-diffing** for the per-code change story.
- If authoritative per-code birth/retirement dates ever become a priority (they
  would cleanly resolve reintroduction and sub-annual timing), the **PCCF** — not
  the NPS — is the source to pursue, contingent on reversing the prior
  no-access decision.
