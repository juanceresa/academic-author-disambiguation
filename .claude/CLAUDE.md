# Academic Author Disambiguation — Project Context

## Pipeline Execution Order (This Is Canon)

1. **OpenAlex broad search** (`src/openalex/query.py`) — First step. Query OpenAlex with `first_name + last_name`. 3-tier matching stores multiple candidate profiles per researcher:
   - Tier 1: Exact name / display_name_alternatives match
   - Tier 2: Bag-of-words name check + institution affiliation overlap
   - Tier 3: Bag-of-words name check + topic overlap with already-confirmed profile

2. **Scopus positional validation** (`src/scopus/id_match.py`) — Second step. For each researcher's Scopus publications:
   - Find researcher's position in Scopus author list (e.g., author #5)
   - Look up same DOI in OpenAlex, grab author at position #5
   - `unique_author_first_appearance = TRUE` on first occurrence of each distinct OpenAlex author ID
   - This confirms which OpenAlex profiles are real and may surface profiles the broad search missed

3. **Document fill / merge** (`src/openalex/documents.py`) — Third step. Reads Scopus→OpenAlex output:
   - Filters to rows where `unique_author_first_appearance == TRUE`
   - Verifies surname matches the OpenAlex profile
   - Fetches ALL works (paginated) from each confirmed profile
   - `all_publications.extend(pubs)` — merges all profiles into one combined list
   - Writes single output Excel per researcher

4. **BigQuery aggregation** (`src/bigquery/matching.py`) — Groups by researcher ID, collects all unique OpenAlex author IDs, sums works/citations

## Key Concepts

- **Profiles are stored separately, merged at researcher level** — the system does NOT blindly merge
- **DOI deduplication** — same paper indexed under two profiles is counted once
- **`unique_author_first_appearance`** — dedup flag so each OpenAlex profile is only fetched once per researcher
- **Scopus is validation, not discovery** — OpenAlex search comes first, Scopus confirms via positional matching
- **Ambiguous cases are flagged for manual review**, not auto-merged

## Spanish Name Handling

- Two surnames (paternal + maternal), databases drop maternal inconsistently
- Accent stripping via Unicode NFD decomposition
- Bag-of-words check handles initials (J. vs Juan) and token order variation

## Demo Notes

- `demo/` directory is gitignored
- `DEMO_PREP.md` is gitignored
- Demo hits OpenAlex API live; does NOT require BigQuery or Scopus API key for basic flow
- `--plain` flag for clean text output, default is rich terminal UI
- `--file researchers.csv` to load researchers from file
