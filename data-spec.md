# Canadian Postal Code Change Tracking — Data Sources & Implementation Reference

**Purpose:** This document provides everything a software engineer needs to implement a tool that tracks historical changes to Canada's ~893,000 six-digit postal codes (additions, retirements, attribute changes) and visualizes them. No further research should be required.

**Last updated:** February 2026

---

## 1. Background: How Canadian Postal Codes Work

A Canadian postal code is a six-character alphanumeric string in the format `A1A 1A1` (letter-digit-letter space digit-letter-digit). It has two parts:

- **FSA (Forward Sortation Area)** — first 3 characters (e.g., `M5V`). Identifies a major geographic area. ~1,675 FSAs exist as of Feb 2026.
- **LDU (Local Delivery Unit)** — last 3 characters (e.g., `1J2`). Narrows to a specific city block, building, or rural route.

Key rules:
- Second character = `0` means **rural** FSA; `1-9` means **urban** FSA.
- Letters D, F, I, O, Q, U are never used (optical scanner confusion).
- First letter encodes province/territory: Ontario uses K, L, M, N, P; Quebec uses G, H, J; etc.
- LDU ending in `0` = Canada Post facility (not residential).
- LDU `9Z9` = Business Reply Mail.
- As of Feb 2026, there are approximately **892,959** unique postal codes across **1,675** FSAs.

**Canada Post** is the sole authority that creates, modifies, and retires postal codes. They do **not** publish any public changelog, bulletin, or gazette of changes. Changes happen silently via monthly data file updates pushed to licensees over SFTP. The only way to detect changes is to diff successive data snapshots.

**"Urbanization"** is Canada Post's term for converting a rural FSA (second char = 0) to urban codes. For example, in 2008, `G0N 3M0` (rural Quebec) was split into multiple `G3N` urban codes. This is one of the more dramatic types of changes.

---

## 2. Data Sources Overview

| # | Source | Type | Coverage | Historical? | Cost | Best For |
|---|--------|------|----------|-------------|------|----------|
| **1** | Statistics Canada — National Address Register (NAR) | Address-level CSV with postal codes | All of Canada, civic addresses only | 6 snapshots (2022–2025) | Free (Open Licence) | **Primary source**: deriving postal code lists at multiple points in time |
| **2** | Statistics Canada — PCCF (Postal Code Conversion File) | Postal-code-to-census-geography linkage | All ~893K postal codes + retired codes | Birth/retirement dates back to 1983 | Restricted (DLI academic or Canada Post licence) | **Gold standard** if accessible: has birth_date, ret_date per postal code |
| **3** | Geocoder.ca | Crowdsourced postal code + lat/lon | ~925K postal codes | Current snapshot only; no archives | Free for non-profits (CC BY 2.5); $808 CAD commercial | Cross-referencing current state; starting monthly snapshots going forward |
| **4** | GeoNames | Postal code extract | Canada (CA_full.csv.zip) | Current only | Free (CC BY 4.0) | Quick validation source |
| **5** | Canada Post — Licensed Data Products | Authoritative postal code address data | All postal codes | Monthly updates; no public archive | Commercial licence required | Authoritative but not freely available |
| **6** | Internet Archive (Wayback Machine) | Archived snapshots of Geocoder.ca/GeoNames | Varies | Sporadic captures | Free | Potentially recovering older snapshots |

---

## 3. Source 1: National Address Register (NAR) — PRIMARY SOURCE

### Overview

The NAR is an authoritative list of valid georeferenced civic addresses in Canada published by Statistics Canada under the **Statistics Canada Open Licence**. Each record includes the full 6-digit postal code. By extracting unique postal codes from multiple NAR snapshots, you can detect additions and removals over time.

### Access

**Catalogue page:** `https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm`

**Direct download URLs (ZIP files containing CSV):**

| Reference Period | URL |
|---|---|
| 2022 | `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/2022.zip` |
| 2023 | `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/2023.zip` |
| June 2024 | `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/2024.zip` |
| December 2024 | `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202412.zip` |
| July 2025 | `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202507.zip` |
| December 2025 | `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202512.zip` |

New releases appear roughly every 6 months. Check the catalogue page for updates.

### Data Schema

The NAR is a CSV file (inside the ZIP) with the following key fields:

| Field | Description | Example |
|---|---|---|
| `addressId` | GUID — unique identifier per address | `21eda105-bb11-4f80-8c5a-fc3523bc9ba0` |
| `locationId` | GUID — unique identifier per physical building | `a3ffb8ed-4040-4b1c-8e26-56e25a88cfd3` |
| `civicNumber` | Street number | `313` |
| `streetName` | Street name | `Doncaster` |
| `streetType` | Abbreviated type | `ST`, `AVE`, `BLVD` |
| `streetDirection` | Direction code | `N`, `S`, `E`, `W`, or blank |
| `cityName` | Municipality | `Winnipeg` |
| `province` | 2-digit province code | `35` (Ontario), `24` (Quebec), etc. |
| `postalCode` | 6-char postal code, no space | `R3N1W7` |
| `latitude` | Decimal degrees | `49.86496` |
| `longitude` | Decimal degrees | `-97.20929` |
| `CSD` | Census Subdivision code | `3520005` (Toronto) |
| `FED` | Federal Electoral District code | `35024` |

**Province codes for filtering:**
- `10` = NL, `11` = PE, `12` = NS, `13` = NB, `24` = QC, `35` = ON, `46` = MB, `47` = SK, `48` = AB, `59` = BC, `60` = YT, `61` = NT, `62` = NU

**User Guide:** `https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022024001-eng.htm`

### File Size & Processing Notes

- Each NAR ZIP is **large** (~1–2 GB uncompressed) because it lists every individual address, not just unique postal codes.
- Canada-wide, expect **~15 million address records** per snapshot.
- To extract a postal code list, you need to `SELECT DISTINCT postalCode, province, cityName, latitude, longitude` (or similar aggregation to get one representative lat/lon per postal code — e.g., the centroid or first occurrence).
- Ontario alone will have ~5–6 million address records yielding ~300,000–350,000 unique postal codes.
- The CSV uses UTF-8 encoding. City names may contain accented characters (French names in Quebec, Ontario, NB).

### Limitations

- The NAR only contains **civic addresses** (physical street addresses). It does NOT include:
  - PO Box postal codes
  - General Delivery codes
  - Large Volume Receiver codes (e.g., big corporations with their own postal code)
  - Rural route codes that lack civic addressing
- Therefore, the NAR will yield a **subset** (~850K–900K) of the total ~893K postal codes. Some postal codes that exist purely for PO Boxes or institutional mail won't appear.
- The NAR's postal code comes from the **mailing address** field, which follows Canada Post guidelines. This is generally reliable.
- No `birth_date` or `retired_date` fields — you must infer changes by diffing snapshots.

### Implementation Approach

```
For each NAR snapshot:
  1. Download and extract ZIP
  2. Load CSV into dataframe
  3. Extract unique postal codes with representative attributes:
     - postalCode
     - province (for provincial filtering/aggregation)
     - cityName (most frequent city for that postal code)
     - latitude/longitude (centroid or first occurrence)
     - CSD code
  4. Store as a timestamped snapshot table

Between any two snapshots:
  - NEW codes = set(later) - set(earlier)
  - RETIRED codes = set(earlier) - set(later)
  - CHANGED codes = codes in both but with different city/province/CSD
```

---

## 4. Source 2: Postal Code Conversion File (PCCF)

### Overview

The PCCF is the **gold standard** for postal code lifecycle data. It is produced by Statistics Canada in collaboration with Canada Post and links every 6-digit postal code to census geographies. Crucially, it includes `Birth_Date` and `Ret_Date` fields for every postal code, allowing you to reconstruct the full history of when codes were created and retired, going back to the 1980s.

### Access — RESTRICTED

The PCCF is **not freely downloadable**. Access requires one of:
- **Data Liberation Initiative (DLI):** Available to Canadian academic institutions. If you have a university affiliation (e.g., University of Toronto), contact the university library's data services. UofT provides access via CHASS Data Centre or the Map & Data Library (requires UTORid login): `https://mdl.library.utoronto.ca/collections/numeric-data/census-canada/postal-code-conversion-file`
- **Community Data Program (CDP):** Membership-based, for non-profits and municipalities: `https://communitydata.ca/data/postal-code-conversion-file-december-2023-update`
- **Canada Post Commercial Licence:** Contact `data.targetingsolutions@canadapost.ca` — pricing not publicly listed.
- **UBC Abacus:** `https://dvrs-applnxprd2.library.ubc.ca/dataset.xhtml?persistentId=hdl:11272.1/AB2/WIWZZX` (DLI access required).

### Versions Available

PCCF versions are released roughly quarterly, aligned with Canada Post's monthly data drops. Major versions align with census years:
- **2021 Census geography:** Current series (March 2022 onward). As of writing, the most recent is **December 2025** update.
- **2016 Census geography:** Previous series (up to November 2020).
- **2011 Census geography and earlier:** Older versions exist in DLI archives.

### Data Schema (2021 Census PCCF)

Fixed-width text file (not CSV). Key fields:

| Field | Position | Length | Description |
|---|---|---|---|
| `Postal Code` | 1–6 | 6 | Six-character postal code (no space) |
| `FSA` | 1–3 | 3 | Forward Sortation Area |
| `PR` | 7–8 | 2 | Province/territory code |
| `CDuid` | 9–12 | 4 | Census Division unique ID |
| `CSDuid` | 13–19 | 7 | Census Subdivision unique ID |
| `CSDname` | 20–89 | 70 | Census Subdivision name |
| `CSDtype` | 90–92 | 3 | CSD type code |
| `DAuid` | varies | 8 | Dissemination Area unique ID |
| `LAT` | varies | 11 | Latitude (decimal degrees) |
| `LONG` | varies | 13 | Longitude (decimal degrees) |
| `SLI` | varies | 1 | Single Link Indicator (1 = best match DA) |
| `DMT` | varies | 1 | Delivery Mode Type (see below) |
| `H_DMT` | varies | 1 | Historic Delivery Mode Type |
| `Birth_Date` | varies | 6 | `YYYYMM` when postal code was created |
| `Ret_Date` | varies | 6 | `YYYYMM` when postal code was retired (blank if active) |
| `Comm_Name` | varies | 30 | Canada Post community name |
| `PCtype` | varies | 1 | Postal code type (1=street, 2=route, 3=LVR, etc.) |
| `QI` | varies | 1 | Quality Indicator |

**Delivery Mode Types (DMT):**
| Code | Description |
|---|---|
| A | Street address (Letter Carrier) |
| B | Street address (Route) |
| C | Lock Box |
| D | Route (Rural) |
| E | General Delivery |
| H | Rural route |
| J | PO Box |
| K | Suburban service |
| W | Rural (StatCan assigned) |
| Z | **Retired** postal code |

**Critical:** A postal code with `DMT = Z` is retired, regardless of whether it's in the main file or the separate retired file.

**Retired file:** Postal codes retired before a cutoff date are in a separate file (`PCCF_retired_FCCP_retraite_*.txt`) following the same layout. This keeps the main file size manageable.

### PCCF Summary Statistics (from Nov 2020 Reference Guide, Table 3.1)

| Province | Unique Codes | Newly Added | Total Retired | Newly Retired |
|---|---|---|---|---|
| Newfoundland & Labrador | 11,481 | 25 | 113 | 1 |
| Prince Edward Island | 4,095 | 13 | 22 | — |
| Nova Scotia | 29,060 | 52 | 281 | 12 |
| New Brunswick | 59,993 | 88 | 854 | 36 |
| Quebec | 220,125 | 400 | 2,396 | 55 |
| Ontario | 290,361 | 552 | 1,957 | 30 |
| Manitoba | 25,853 | 249 | 130 | 9 |
| Saskatchewan | 22,950 | 40 | 65 | 2 |
| Alberta | 89,150 | 520 | 774 | 32 |
| British Columbia | 119,093 | 265 | 540 | 46 |
| Yukon | 1,016 | 2 | 4 | — |
| Northwest Territories | 542 | 1 | — | — |
| Nunavut | 28 | — | — | — |
| **Total** | **873,747** | **2,207** | **7,136** | **223** |

This table is from a single point-in-time release. Each PCCF release has its own version of this table in its reference guide.

### Implementation Notes

If you can obtain PCCF access:
- Parse the fixed-width text file using positional offsets from the reference guide.
- Filter to `SLI = 1` to get one "best" record per postal code.
- Use `Birth_Date` and `Ret_Date` for historical lifecycle analysis.
- Use `DMT` to classify codes (street vs PO Box vs rural route vs retired).
- Some codes have been retired and **reintroduced** — `Birth_Date` may reflect the most recent activation.
- The PCCF is the only source that explicitly tells you *when* a code was born and when it died.

---

## 5. Source 3: Geocoder.ca — Crowdsourced Dataset

### Overview

Geocoder.ca is a Canadian crowdsourced geocoding service that maintains a dataset of ~925,000 postal codes with lat/lon coordinates. It is updated monthly on the 1st of each month. The dataset is free for non-profits under Creative Commons Attribution 2.5 Canada.

### Access

**Free data page:** `https://geocoder.ca/?freedata=1`

**Licence:**
- Non-profit use: Free (CC BY 2.5 Canada)
- Commercial use: $808 CAD one-time + $100/month for updates

**Download:** The free dataset is a CSV available for download from the page above. Registration may be required.

### Data Schema (Summary File — Dataset #1)

| Field | Description | Example |
|---|---|---|
| `PostCode` | 6-char postal code, no space | `M5V1J2` |
| `Latitude` | Decimal degrees | `43.643510` |
| `Longitude` | Decimal degrees | `-79.392235` |
| `City` | City name | `Toronto` |
| `Province` | 2-letter province code | `ON` |
| `CityAlt` | Alternate city name | — |
| `Neighborhood` | Neighbourhood name | `Entertainment District` |
| `Time Zone` | UTC offset | `UTC-05:00` |
| `Name` | IANA timezone name | `America/Toronto` |
| `Area Code` | Phone area code | `416` |

### Limitations

- **No historical archives.** Geocoder.ca only provides a current snapshot. Past versions are not available for download.
- **Crowdsourced accuracy.** Not authoritative — Canada Post doesn't endorse it. Some entries may have errors, lag behind CPC updates, or be missing.
- **No birth/retirement dates.**
- **No census geography linkage.**

### Implementation Approach

- Download the current snapshot as a baseline.
- Set up a **monthly cron job** to re-download on the 1st of each month and archive each snapshot with a date stamp.
- Diff successive monthly snapshots to detect new and removed postal codes on a monthly basis.
- This gives you **going-forward** monthly resolution, but no retrospective history.

### Wayback Machine Recovery

You can attempt to recover older snapshots of Geocoder.ca's free data page from the Internet Archive:
- `https://web.archive.org/web/*/geocoder.ca/?freedata=1`
- The downloadable CSV file itself may or may not have been captured. Worth checking, but don't count on it.

---

## 6. Source 4: GeoNames Postal Code Data

### Overview

GeoNames aggregates global postal code data under CC BY 4.0 licence. For Canada, they provide a file with full 6-digit postal codes.

### Access

**Download page:** `https://download.geonames.org/export/zip/`
**Canada full file:** `https://download.geonames.org/export/zip/CA_full.csv.zip`

### Data Schema

Tab-delimited text file:

| Field | Description |
|---|---|
| country code | `CA` |
| postal code | 6-char code, with space (e.g., `M5V 1J2`) |
| place name | City/town name |
| admin name1 | Province name |
| admin code1 | Province abbreviation |
| admin name2 | — (empty for Canada) |
| admin code2 | — |
| admin name3 | — |
| admin code3 | — |
| latitude | Decimal degrees |
| longitude | Decimal degrees |
| accuracy | Precision indicator (1, 4, or 6) |

### Limitations

- No historical archives (current state only).
- Coordinates are often approximated from place name matching, not street-level.
- No birth/retirement dates, no census geography.
- Useful only as a supplementary cross-reference.

---

## 7. Source 5: Canada Post Licensed Data Products

### Overview

Canada Post sells the authoritative, comprehensive postal code dataset through commercial licences. Their "Postal Code Address Data" file is the master list of all postal codes with associated address ranges, updated **monthly** via SFTP.

### Product Details

| Product | Description |
|---|---|
| **Postal Code Address Data** | All postal codes and their associated address ranges. The definitive list. |
| **Delivery Mode Data** | Presort information for mail. |
| **Householder Data** | Count of houses/apartments/farms/businesses per FSA. |
| **Householder Elite Data** | Same as above but at full postal code level. |
| **Postal Code Lat/Long Data** | Coordinates at postal code level. |

### Release Schedule

Canada Post publishes a **Data Production Schedule** each year. In 2025, files were posted to SFTP approximately monthly:
- Dec 6, 2024; Jan 3, 2025; Feb 7, 2025; Mar 7, 2025; Apr 4, 2025; May 2, 2025; Jun 6, 2025; Jul 4, 2025; Aug 1, 2025; Sep 5, 2025; Oct 3, 2025; Nov 7, 2025; Dec 5, 2025; Jan 9, 2026.

File naming convention: `YYMMDD_ad.zip` (e.g., `250103ad.zip` for the Jan 3, 2025 Postal Code Address Data).

### Access

- Contact: `data.targetingsolutions@canadapost.ca` or call `1-877-281-4137`
- Request form: `https://www.canadapost-postescanada.ca/cpc/doc/en/business/request-for-licensed-data-products-form-en.pdf`
- Technical specs: `https://origin-www.canadapost.ca/cpc/doc/en/business/postalcodetechspecs.pdf`
- Pricing: Not publicly listed; varies by use case.
- Licence restrictions: Canada Post claims copyright over postal code data. Licensees cannot redistribute.

### Implementation Notes

If a licence is obtained:
- Archive each monthly SFTP drop.
- Diff successive months to generate a precise changelog.
- This is the only way to get authoritative monthly-resolution change data.

---

## 8. Recommended Implementation Architecture

### Phase 1: Build the Historical Baseline (NAR-Based)

Since the NAR is the only free, open, historical source with full 6-digit postal codes, use it as the foundation.

```
Step 1: Download all 6 NAR snapshots
Step 2: For each, extract unique postal codes with attributes
Step 3: Store in a database with schema:

  TABLE postal_code_snapshots (
    postal_code     CHAR(6),        -- e.g., 'M5V1J2'
    snapshot_date   DATE,           -- e.g., '2022-01-01'
    province_code   CHAR(2),        -- e.g., '35'
    city_name       VARCHAR(100),   -- e.g., 'Toronto'
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    csd_code        VARCHAR(10),    -- Census Subdivision
    address_count   INTEGER         -- how many NAR addresses use this code
  )

Step 4: Generate change events:

  TABLE postal_code_changes (
    postal_code     CHAR(6),
    change_type     ENUM('added', 'removed', 'city_changed', 'csd_changed'),
    detected_between_start  DATE,   -- earlier snapshot date
    detected_between_end    DATE,   -- later snapshot date
    old_value       TEXT,           -- previous city/CSD if changed
    new_value       TEXT,           -- new city/CSD if changed
    province_code   CHAR(2),
    fsa             CHAR(3)         -- first 3 chars, for aggregation
  )
```

### Phase 2: Ongoing Monthly Monitoring (Geocoder.ca)

Set up automated monthly collection from Geocoder.ca to detect changes going forward at monthly resolution.

```
Monthly cron job (1st of each month):
  1. Download Geocoder.ca summary CSV
  2. Store as new snapshot
  3. Diff against previous month
  4. Generate change events
```

### Phase 3: Enrich with PCCF (if accessible)

If PCCF access is obtained, load the birth_date and ret_date fields to backfill the full lifecycle history for every postal code.

### Phase 4: Visualization

**Suggested visualizations:**

1. **Map view:** Choropleth of FSAs coloured by number of changes (additions, removals) in a selected time period. Use Leaflet/MapLibre with StatCan FSA boundary shapefiles.

2. **Timeline view:** Line chart of total active postal codes over time, by province. Bar chart of monthly additions vs removals.

3. **Change log table:** Searchable, filterable table of all detected changes with postal code, type, date range, province, city.

4. **FSA drill-down:** Click an FSA on the map to see all 6-digit codes within it and their status (active, added, removed).

**Map boundary data for visualization:**

StatCan publishes FSA boundary shapefiles for each census year:
- 2021: `https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm?year=21`
  - Select "Forward Sortation Areas" under type.
  - Download as Shapefile or GeoJSON.
- These are the 3-character FSA boundaries (not 6-digit), suitable for choropleth maps.

---

## 9. Province-Level FSA Reference

For filtering and display purposes, the first character of a postal code maps to provinces as follows:

| First Letter | Province/Territory |
|---|---|
| A | Newfoundland and Labrador |
| B | Nova Scotia |
| C | Prince Edward Island |
| E | New Brunswick |
| G | Eastern Quebec |
| H | Montreal metropolitan |
| J | Western Quebec |
| K | Eastern Ontario |
| L | Central Ontario |
| M | Toronto metropolitan |
| N | Southwestern Ontario |
| P | Northern Ontario |
| R | Manitoba |
| S | Saskatchewan |
| T | Alberta |
| V | British Columbia |
| X | Northwest Territories / Nunavut |
| Y | Yukon |

**Letters not used as first character:** D, F, I, O, Q, U, W, Z

**Nunavut vs NWT disambiguation (both use X):**
- Nunavut: X0A, X0B, X0C
- NWT: X0E, X0G, X1A

**Special codes:**
- `H0H 0H0` — Santa Claus (not a real delivery area)
- `K1A` — Federal government offices (mostly Ottawa, but ~16 codes physically in Gatineau, QC)
- `M0R` — Commercial returns processing (Gateway, Mississauga)
- `T0W` — Commercial returns processing (Calgary)

---

## 10. Expected Scale of Changes

Based on the PCCF Nov 2020 data, approximately **2,200 postal codes are added** and **200 are newly retired** in a typical year across Canada. Ontario alone sees ~550 additions per year.

Types of changes to expect:
- **New construction:** New subdivisions, condo towers, and commercial developments generate new postal codes. This is the bulk of additions.
- **Urbanization:** A rural FSA (x0x) is replaced by urban codes. The old rural code is retired and dozens/hundreds of new urban codes are created. Example: G0N→G3N in 2008.
- **Retirement:** Old codes are retired when delivery points are consolidated or addresses change. Retired codes may be **reintroduced** later.
- **Attribute changes:** A postal code's associated city name or census subdivision may change due to municipal amalgamation/restructuring without the code itself changing.

---

## 11. Licensing Summary

| Source | Licence | Can Redistribute? | Can Use Commercially? |
|---|---|---|---|
| NAR | Statistics Canada Open Licence | Yes | Yes |
| PCCF | DLI / Canada Post End-Use Licence | No | Depends on licence terms |
| Geocoder.ca | CC BY 2.5 Canada | Yes (with attribution) | Non-profit free; commercial $808 CAD |
| GeoNames | CC BY 4.0 | Yes (with attribution) | Yes |
| Canada Post Licensed Data | Proprietary | No | Licence-specific |
| StatCan FSA Boundary Files | Statistics Canada Open Licence | Yes | Yes |

For a tool that will be publicly accessible, the **NAR + Geocoder.ca + GeoNames + StatCan boundary files** combination is the safest open-licence stack.

---

## 12. Quick-Start Checklist

- [ ] Download all 6 NAR snapshots from StatCan (URLs in Section 3)
- [ ] Download current Geocoder.ca CSV (Section 5)
- [ ] Download GeoNames CA_full.csv.zip (Section 6)
- [ ] Download 2021 FSA boundary shapefile from StatCan (Section 8)
- [ ] Set up database schema (Section 8)
- [ ] Write NAR parser: extract unique postal codes per snapshot
- [ ] Write differ: compare consecutive snapshots, generate change events
- [ ] Set up monthly Geocoder.ca cron job
- [ ] Build map visualization with FSA boundaries
- [ ] Build timeline charts and change log UI
- [ ] (Optional) Obtain PCCF via DLI and backfill birth/retirement dates