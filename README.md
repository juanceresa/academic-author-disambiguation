# Academic Author Disambiguation

Pipeline for matching Fulbright scholars to their academic publications across OpenAlex, Scopus, and Google Scholar using fuzzy name matching, institutional affiliation signals, and cross-database verification. Built during an internship at EC3 Research Group (University of Granada).

## Problem

Researchers publish under name variants across multiple databases. Matching a scholar to their complete publication record requires solving several challenges:

- **Name ambiguity**: Common names produce hundreds of false positives
- **Spanish naming conventions**: Two surnames (paternal + maternal) are handled inconsistently across databases — some drop the maternal surname, others abbreviate
- **Incomplete coverage**: No single database indexes all publications. OpenAlex, Scopus, and Google Scholar each have blind spots
- **Scale**: The pipeline processes thousands of researchers against hundreds of millions of academic works

## Approach

### Multi-tier matching (OpenAlex)

The core matching engine uses a 3-tier strategy against the OpenAlex API:

1. **Exact match**: Display name or known alternatives match the researcher name exactly
2. **Institution match**: Bag-of-words name verification + institutional affiliation overlap
3. **Topic match**: When name is plausible, check if research field aligns with an already-verified profile

### Cross-database coverage

To maximize recall, the pipeline queries multiple sources:

- **OpenAlex API** — Primary source. Open, comprehensive, best for name disambiguation
- **Scopus (Elsevier API)** — Author profiles with institutional history. DOI-based position matching maps Scopus authors to OpenAlex IDs
- **CrossRef** — DOI discovery scored by name match quality, affiliation overlap, and publication year proximity

Google Scholar scraping was attempted (via Oxylabs proxy) but hit persistent rate-limiting walls.

### Fuzzy name matching

All name comparisons use normalized, accent-stripped tokens with configurable fuzzy thresholds (default 90%). Handles:

- Accent variations (García → garcia)
- Hyphenated names (Acín-Pérez → acin perez)
- Abbreviated first names (J. → matched against first initial)
- Missing maternal surnames

## Pipeline Architecture

```
Excel inputs (researcher lists)
        │
        ├──→ Scopus API ──→ Author profiles + publication lists
        │                         │
        │                         ├──→ DOI matching ──→ OpenAlex author IDs
        │                         │
        ├──→ OpenAlex API ──→ 3-tier candidate matching
        │                         │
        ├──→ CrossRef API ──→ DOI discovery (scored)
        │
        └──→ BigQuery (central store) ──→ Aggregated results ──→ Excel outputs
```

## Tech Stack

- **Python 3.12+**
- **Google BigQuery** — Central data store for researcher tables and OpenAlex snapshots
- **OpenAlex API** — Author and works search
- **Scopus/Elsevier API** (via elsapy) — Author profiles and publication metadata
- **CrossRef API** — DOI discovery and validation
- **pandas** — Data manipulation and Excel I/O
- **fuzzywuzzy** — Fuzzy string matching for name comparison
- **pydantic-settings** — Configuration management

## Project Structure

```
├── src/
│   ├── config.py                  # Settings from .env
│   ├── common/
│   │   └── name_matching.py       # Shared name normalization and matching
│   ├── openalex/
│   │   ├── query.py               # 3-tier author matching
│   │   └── documents.py           # Publication retrieval and validation
│   ├── scopus/
│   │   ├── query.py               # Elsevier API author search
│   │   └── id_match.py            # DOI-based Scopus → OpenAlex mapping
│   ├── bigquery/
│   │   ├── query_db.py            # DOI and API-based matching via BigQuery
│   │   └── matching.py            # Cross-database matching and compilation
│   └── google_scholar/
│       ├── search.py              # CrossRef DOI search with scoring
│       └── scrape.py              # Researcher spreadsheet processing
├── scripts/
│   ├── run_openalex.py            # Run OpenAlex matching pipeline
│   ├── run_scopus.py              # Run Scopus search or ID matching
│   ├── run_scholar.py             # Run CrossRef DOI discovery
│   └── run_pipeline.py            # Pipeline overview and execution order
├── samples/                       # Example output files
├── pyproject.toml                 # Dependencies and project metadata
└── .env.example                   # Environment variable template
```

## Setup

```bash
# Clone and install
pip install -e .

# Configure environment
cp .env.example .env
# Edit .env with your API keys:
#   SCOPUS_API_KEY=...
#   BIGQUERY_PROJECT=...
#   CROSSREF_EMAIL=...

# BigQuery authentication
gcloud auth application-default login
```

## Usage

Each pipeline stage runs independently:

```bash
# 1. Search Scopus for author profiles
python scripts/run_scopus.py --mode search

# 2. Match researchers to OpenAlex profiles (3-tier matching)
python scripts/run_openalex.py

# 3. Map Scopus DOIs to OpenAlex author IDs
python scripts/run_scopus.py --mode id-match

# 4. Search CrossRef for additional DOIs
python scripts/run_scholar.py --input researchers.xlsx
```

## Validation

Outputs were validated by manual inspection of Excel exports against known researcher profiles. The matching pipeline was cross-referenced across multiple databases — when OpenAlex, Scopus, and CrossRef agreed on an author match, confidence was high. Edge cases (common names, missing institutions) were flagged for manual review.

## Lessons Learned

- **OpenAlex was the most reliable source** for open author disambiguation at scale. Its display_name_alternatives field and institution affiliations made matching significantly more accurate than name-only approaches
- **Institution ID caching** was critical for API efficiency — without it, the same institution would be looked up thousands of times across researchers
- **Spanish name normalization** was the hardest subproblem. Two-surname conventions vary across databases, and accent handling required careful Unicode decomposition
- **Google Scholar scraping doesn't scale** — even with proxy services (Oxylabs), rate limiting made it impractical for batch processing. CrossRef proved a more reliable alternative for DOI discovery
- **80% solution shipping**: The 3-tier matching catches the vast majority of researchers. Rather than building a perfect disambiguation system, flagging uncertain matches for manual review was more effective
