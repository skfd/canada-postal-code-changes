# Wikipedia — Postal codes in Canada

- **URL:** https://en.wikipedia.org/wiki/Postal_codes_in_Canada
- **Type:** General-audience encyclopedic reference
- **Reviewed:** 2026-07-20 (full article read via browser)

*Read if you need the canonical origin story, a reliable definition of any structural
term (FSA/LDU/urbanization), or a citable secondary source — this is the anchor
reference; link here instead of re-explaining history on our own site.*

---

## Format (the exact rules)

- Form: **A1A 1A1** — letter-digit-letter, space, digit-letter-digit.
- Canada Post mandates **exactly one space**, uppercase, **no hyphens**. From a
  computing standpoint the string is strictly **7 characters**.
- The code follows the province/territory abbreviation in a written address.
- As of Oct 2019: **876,445** postal codes; FSAs range A0A (Newfoundland) to
  Y1A (Yukon). (Our project's ~893K July-2026 figure reflects growth since.)

## History (the part that's "solved" for general readers)

**City postal zones (1925–1969):** Numbered zones started in **Toronto, 1925**
("Toronto 5, Ontario"). By 1943 Toronto had 14 zones. Montreal added zones in
1944; by the early 1960s Quebec, Ottawa, Winnipeg, Vancouver, Toronto, Montreal
all used them. Late 1960s: a **three-digit** zone scheme began replacing the
one/two-digit ones (Montreal, Toronto, Vancouver). Toronto's renumbering took
effect **1 May 1969** with the slogan "Your number is up" — but was cancelled
almost immediately once the national code was announced, so businesses paid twice.

**Planning (1969–1970):** A 1969 House of Commons report flagged "environmental
change" pressure and recommended a task force on automation, possibly a postal
code. **Dec 1969:** Communications Minister **Eric Kierans** announced a
six-character code to supersede the three-digit zones. Feb 1970 report:
"A Canadian Public Address Postal Coding System" (Samson, Belair, Simpson,
Riddell Inc.).

**Implementation (1971–1974):** Canada was **one of the last Western countries**
to adopt a national code. Trial in **Ottawa, 1 April 1971** → provincial rollout
in Manitoba → rest of country 1972–74. End-of-1974 national uptake was only
**38.2%**. Sorting machines could then handle 26,640 items/hour.

**Union resistance:** CUPW opposed automation (machine operators paid less than
hand-sorters), ran a "**don't use postal codes**" public campaign, and declared
**20 March 1975 "Boycott the Postal Code" Day** (also demanding a 40→30-hour
week). Boycott called off Feb 1976 after a new collective agreement. (Colourful
detail: a controversial 1975 "thong" ad denounced in the House of Commons.)

## Structure — the two components

**Forward Sortation Area (FSA) = first 3 chars.** All codes in an FSA share these.
- **1st char (letter):** postal district → outside QC/ON = a whole province/
  territory. QC split into 3 districts, ON into 5. Dedicated metro districts:
  **H = Montreal, M = Toronto**. NU + NWT **share X** (even post-1999 split).
- **2nd char (digit):** urban vs rural. **0 = wide-area rural** (or, rarely, a
  special-purpose code); any other digit = urban.
- **3rd char (letter):** a specific rural region, a whole medium city, or a
  section of a metro area. In downtown Toronto/Montreal/Vancouver some FSAs map
  to a **single building**. Rural FSAs vary wildly (NWT X0G = just Fort Liard;
  X0E = every other NWT community except Yellowknife).

**Local Delivery Unit (LDU) = last 3 chars.** A specific address or address
range: a small town, part of a town, one side of a city block, a large building
or part of one, an institution (university/hospital), or a high-volume business.
- **LDU ending in 0** → postal facilities (post offices up to sortation plants).
- **9Z9** → Business Reply Mail exclusively.
- **9Z0** → large regional distribution facilities / placeholder (e.g. the old
  "K0H 9Z0" on local Kingston mail).
- Rural: LDU can be a set of PO boxes or a rural route.

## Address space & scarcity

- Uses **20 letters** (excludes D, F, I, O, Q, U); 1st position also excludes W, Z.
- Max FSAs = 18×10×20 = **3,600**; max LDUs per FSA = 2,000 → theoretical
  **7.2 million** codes. StatCan counted ~830,000 active (~12% of space) → lots
  of LDU room. **FSAs are the scarce resource:** as of 2024 only **three** BC
  FSAs remained unused (V3P, V4H, V4J).

## Urbanization & retirement (CORE to our project)

- **"Urbanization"** = Canada Post replacing a rural code (2nd char = 0) with
  urban codes once a community passes a population threshold (other factors too).
- **The vacated rural code can then be reassigned to another community OR
  retired.** ← this is exactly the churn we diff and visualize.
- Worked example: **early 2008, G0N 3M0** (Sainte-Catherine-de-la-Jacques-Cartier,
  Fossambault-sur-le-Lac, Lac-Saint-Joseph, QC) urbanized to **G3N** codes to
  remove street-name ambiguities.
- **New Brunswick (district E) is completely urbanized** — its rural codes were
  phased out entirely. (A useful edge case for our per-FSA views.)

## Notable special codes (good "hooks" for site copy)

- **H0H 0H0** — Santa Claus, North Pole ("Ho ho ho"). H0- is an anomaly (rural 0
  under Montreal's H); nearly empty — only other active H0- is **H0M** (Akwesasne
  reserve). Official Santa program since 1983; ~1M letters/year.
- **M0R / T0W** — reserved for freepost "Commercial Returns" (e.g. Amazon, the
  Shopping Channel).
- Military transition codes: **V9A 7N2** (FMO Victoria), **B3K 5X5** (FMO
  Halifax), **K8N 5W6** (CFPO Belleville).
- **A1A 1A1** — often shown as a generic example but is a *real* code (Lower
  Battery, St. John's, NL). **K1A 0B1** = Canada Post HQ, Ottawa.

## Alternative uses

- Postal codes correlate with census/health-registry data to build geographic
  population profiles (e.g. childhood-cancer risk studies) — the analytic use
  case that motivates StatCan's PCCF.
- Electoral districts often follow postal areas → MP lookup by postal code.

## Why this matters for us

- The **history is fully covered here** → we link, we don't re-tell.
- The article confirms our thesis: **urbanization + retirement + reassignment**
  are real, ongoing, and only lightly explained — with a single worked example
  (G0N→G3N) and no longitudinal view. That churn is our whole product.
- Good vocabulary source: "postal district", "wide-area rural", "urbanization",
  "LDU", the reserved/special codes — use these terms precisely in our copy.

## Leads worth chasing (from its references / external links)

- **Canadian Postal Museum — "A Chronology of Canadian Postal History: The Postal
  Code"** (archived) — primary historical timeline.
- **StatCan PCCF Reference Guide (Oct 2010), p.46** — cited for urbanization.
- **National Presortation Schematic** — "monthly bulletin detailing postal code
  changes" (Canada Post). Potentially a direct feed of the changes we diff.
- **StatCan Open Database of Addresses (ODA)** — open-licensed, includes postal codes.
- **Doug Ewell's / Frank da Cruz's** Canadian postal code explainer pages.
