# BigQuery Academic Research Data Pipeline

A comprehensive data pipeline for matching and analyzing academic researchers across multiple scholarly databases (OpenAlex, Scopus, Google Scholar) using BigQuery, API integrations, and fuzzy matching techniques.

## 🎯 Project Overview

This project addresses the challenge of researcher identity resolution across different academic databases. It implements a multi-stage pipeline that:

- **Identifies researchers** across OpenAlex, Scopus, and other academic databases
- **Matches publications** using DOI-based lookups and fuzzy name matching
- **Analyzes publication patterns** and citation metrics
- **Generates comprehensive reports** for each researcher's academic output

## 🏗️ Architecture

The pipeline consists of several interconnected scripts that process data through BigQuery and external APIs:

```
Data Sources → API Processing → BigQuery Storage → Analysis & Reporting
     ↓              ↓               ↓                    ↓
- Scopus API    - Name matching  - Centralized      - Excel reports
- OpenAlex API  - DOI resolution   storage          - Citation analysis
- Internal DB   - Fuzzy scoring  - Query interface  - Publication lists
```

## 📁 Repository Structure

```
scripts/
├── alex_documents_query.py          # OpenAlex publication retrieval
├── compile_script.py                # Data aggregation and compilation
├── open_alex_document_fill.py       # Detailed OpenAlex data processing
├── open_alex_query.py               # OpenAlex researcher identification
├── query_db.py                      # Initial database matching
├── query_db2.py                     # Enhanced matching with API fallback
├── scopus_database_matching.py     # Scopus-OpenAlex cross-referencing
├── scopus_id_match.py               # Scopus author position mapping
└── scopus_query.py                  # Scopus API data collection
```

## 🚀 Getting Started

### Prerequisites

- **Google Cloud Platform** account with BigQuery access
- **API Keys:**
  - Scopus/Elsevier API key
  - OpenAlex API access (email required)
- **Python Dependencies:**
  ```bash
  pip install pandas google-cloud-bigquery requests fuzzywuzzy
  pip install openpyxl xlsxwriter elsapy unidecode
  ```

### Configuration

1. **Set up Google Cloud credentials:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
   ```

2. **Configure API access:**
   - Add your Scopus API key to `scopus_query.py`
   - Set your email in OpenAlex API calls (required for polite pool access)

3. **Update BigQuery project ID:**
   ```python
   client = bigquery.Client(project="your-project-id")
   ```

## 📊 Core Workflows

### 1. Initial Researcher Identification

**Script:** `open_alex_query.py`

Identifies potential researcher profiles in OpenAlex using multiple matching strategies:
- **Exact name matching** (primary)
- **Institution-based filtering** (secondary)
- **Research field alignment** (tertiary validation)

```python
# Example usage - processes all researchers in the database
python scripts/open_alex_query.py
```

### 2. Publication Data Collection

**Script:** `alex_documents_query.py`

Retrieves comprehensive publication lists for each identified researcher:
- Fetches all works from OpenAlex API
- Extracts metadata (title, DOI, year, citations)
- Generates individual Excel files per researcher

### 3. Cross-Database Validation

**Script:** `scopus_database_matching.py`

Validates researcher matches using Scopus publication data:
- DOI-based cross-referencing
- Author position verification
- Citation count validation

### 4. Enhanced Document Processing

**Script:** `open_alex_document_fill.py`

Advanced processing with author verification:
- Validates author profiles against researcher surnames
- Handles pagination for complete publication lists
- Flags unique author appearances

```bash
python scripts/open_alex_document_fill.py --clear-dir
```

## 🔧 Key Features

### Fuzzy Name Matching
Advanced name normalization and matching algorithms:
- Unicode accent removal
- Punctuation standardization
- Token-based similarity scoring
- Configurable similarity thresholds

### API Rate Limiting
Built-in safeguards for external API calls:
- Retry logic with exponential backoff
- SSL error handling
- Request throttling to respect API limits

### Data Quality Assurance
Multiple validation layers:
- Cross-database verification
- Surname validation for author profiles
- Publication count reconciliation
- Citation metric verification

## 📈 Output Formats

### Individual Researcher Reports
- **Excel files** with publication lists and metadata
- **Summary sheets** with aggregated metrics
- **Detailed works** with citation information

### Aggregated Data Tables
- **BigQuery tables** for large-scale analysis
- **Compiled datasets** for cross-researcher comparisons
- **Validation reports** for data quality assessment

## 🛠️ Database Schema

### Primary Tables

**`investigadores_alexapi_3`**
- Researcher profiles with OpenAlex IDs
- Citation counts and work counts
- Research field classifications

**`scopus_table`**
- Scopus researcher information
- Institutional affiliations
- Scholarship/grant information

**`investigadores_template`**
- Master researcher list
- Name variations and identifiers
- Geographic and institutional data

## 🔍 Usage Examples

### Process a specific researcher
```python
# Search for a researcher by name and institution
search_openalex(fs_id="12345", q_name="John Smith", 
                full_name="John Michael Smith", 
                pais="USA", ins="University of California")
```

### Generate publication reports
```python
# Create Excel reports for all researchers
python scripts/alex_documents_query.py
```

### Validate cross-database matches
```python
# Run Scopus validation
python scripts/scopus_database_matching.py
```

## 📝 Configuration Files

### Required Configuration
- `config.json` - Scopus API configuration
- Google Cloud credentials
- BigQuery project settings

### Optional Settings
- Output directory customization
- API rate limiting parameters
- Fuzzy matching thresholds

## 🤝 Contributing

When contributing to this project:

1. **Test with sample data** before running on full dataset
2. **Update table schemas** in BigQuery as needed
3. **Document any new API integrations**
4. **Follow the existing error handling patterns**

## 📚 Dependencies

### Core Libraries
- `pandas` - Data manipulation and analysis
- `google-cloud-bigquery` - BigQuery integration
- `requests` - HTTP API calls
- `fuzzywuzzy` - String similarity matching

### Specialized Libraries
- `elsapy` - Elsevier/Scopus API wrapper
- `openpyxl` - Excel file generation
- `unidecode` - Unicode text normalization

## ⚠️ Important Notes

- **API Rate Limits:** Be mindful of daily/hourly limits for external APIs
- **Data Privacy:** Ensure compliance with institutional data policies
- **Cost Management:** Monitor BigQuery usage and optimize queries
- **Backup Strategy:** Regularly backup processed results and configurations

## 📞 Support

For questions about specific scripts or data processing issues:
- Check BigQuery logs for processing errors
- Verify API key configurations
- Review output directories for partial results
- Consult API documentation for rate limit guidance

---

**Project Status:** Completed
**Last Updated:** May 2025  
**Maintainer:** Juan Ceresa
