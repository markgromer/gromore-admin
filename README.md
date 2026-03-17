# Home Services Ad Agency - Analytics & Reporting System

A complete analytics platform for home services ad agencies. Ingests exports from Google Analytics, Meta Business Suite, and Google Search Console - then interprets the data, generates month-over-month comparisons, makes actionable improvement suggestions, and produces two report types:

1. **Internal Team Reports** - Detailed tactical reports for the ad account team
2. **Client-Facing Reports** - Clean, professional monthly reports for clients

## Quick Start

```bash
pip install -r requirements.txt
```

## Folder Structure

```
data/
  imports/
    {client_name}/
      {YYYY-MM}/
        google_analytics.csv
        meta_business.csv
        search_console.csv
  database/
    agency.db

reports/
  {client_name}/
    {YYYY-MM}/
      internal_report.html
      internal_report.pdf
      client_report.html
      client_report.pdf

config/
  clients.json
  benchmarks.json
```

## Usage

### 1. Import Data

Drop CSV exports into the correct folder:
```
data/imports/{client_name}/{YYYY-MM}/
```

For example:
```
data/imports/ace_plumbing/2026-03/google_analytics.csv
data/imports/ace_plumbing/2026-03/meta_business.csv
data/imports/ace_plumbing/2026-03/search_console.csv
```

### 2. Add Client Config

Edit `config/clients.json` to add the client:
```json
{
  "ace_plumbing": {
    "display_name": "Ace Plumbing Co.",
    "industry": "plumbing",
    "monthly_budget": 5000,
    "website": "https://aceplumbing.com",
    "goals": ["increase_leads", "reduce_cpa"]
  }
}
```

### 3. Run Reports

```bash
# Process a specific client for a specific month
python run.py --client ace_plumbing --month 2026-03

# Process all clients for the current month
python run.py --all

# Process a client and only generate internal reports
python run.py --client ace_plumbing --month 2026-03 --report-type internal

# Process a client and only generate client reports
python run.py --client ace_plumbing --month 2026-03 --report-type client
```

### 4. View Reports

Reports are saved to `reports/{client_name}/{YYYY-MM}/`. Open the HTML files in a browser or use the PDF versions for delivery.

## Supported Export Formats

### Google Analytics
Export from GA4: Reports > Acquisition > Traffic acquisition > Export as CSV

Expected columns (flexible - the system maps common variations):
- Date, Sessions, Users, New Users, Bounce Rate, Pages/Session, Avg Session Duration
- Source/Medium, Conversions, Revenue

### Meta Business Suite
Export from Meta Business Suite: Ads Manager > Export

Expected columns:
- Campaign Name, Ad Set Name, Impressions, Reach, Clicks, CTR, CPC, CPM
- Spend, Results, Cost Per Result, Frequency

### Google Search Console
Export from GSC: Performance > Export

Expected columns:
- Query, Page, Clicks, Impressions, CTR, Position

## Home Services Industries Supported

The benchmarks and suggestions engine is tuned for:
- Plumbing
- HVAC
- Electrical
- Roofing
- Landscaping
- Pest Control
- Cleaning Services
- General Contracting
- Painting
- Garage Door
- Foundation Repair
- Water Damage Restoration
