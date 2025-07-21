"""
core/sec_filings_fetcher.py
Production module: monitors 13D/13G/A filings for a closed-end-fund universe.

Author: Maya Gil
"""

import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# ────────────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────────────
USER_AGENT = (
    "CEF Dashboard (contact: mg4614@columbia.edu)"  # comply with SEC rule
)
REQUEST_WAIT = 0.11  # 10 requests / second safety margin

# 13D/13G form codes accepted
TARGET_FORMS = {
    "SC 13D", "SC 13D/A",
    "SC 13G", "SC 13G/A",
    "13D", "13D/A", "13G", "13G/A",
     "Schedule 13D", "Schedule 13D/A", "Schedule 13G", "Schedule 13G/A",
}

# Known activist investors and common aliases
ACTIVIST_ALIASES = {
    "Saba Capital":        ["Saba Capital", "Boaz Weinstein"],
    "Karpus Investment":   ["Karpus", "Karpus Investment"],
    "Bulldog Investors":   ["Bulldog Investors", "Phillip Goldstein"],
    "Elliott Management":  ["Elliott", "Paul Singer"],
    "Starboard Value":     ["Starboard Value", "Jeff Smith"],
    "ValueAct Capital":    ["ValueAct"],
    "Third Point":         ["Third Point", "Dan Loeb"],
    "Pershing Square":     ["Pershing Square", "Bill Ackman"],
    "Trian Partners":      ["Trian", "Nelson Peltz"],
}

# CEF ticker → (CIK, Fund Name) mapping —
# build once at start-up; override with your own CSV if desired.
DEFAULT_TICKER_MAP = {
    "PDO": ("0001798618", "PIMCO Dynamic Income Opportunities Fund"),
    "PDI": ("0001510599", "PIMCO Dynamic Income Fund"),
    "PHK": ("0001219360", "PIMCO High Income Fund"),
    "BST": ("0001616678", "BlackRock Science & Tech Trust"),
    "BDJ": ("0001332283", "BlackRock Enhanced Equity Dividend Trust"),
    "JFR": ("0001276533", "Nuveen Floating Rate Income Fund"),
    "ETG": ("0001270523", "Eaton Vance Tax-Advantaged Global Dividend Opportunities"),
    "ASA": ("0001230869", "ASA Gold and Precious Metals Limited"),
    "SWZ": ("0000813623", "Swiss Helvetia Fund"),
    "ECF": ("0000793040", "Ellsworth Growth & Income Fund Ltd"),
    "BCV": ("0000009521", "Bancroft Fund Ltd"),
    "NBXG": ("0001843181", "Neuberger Berman NextGen Connectivity Fund Inc."),
    "JOF": ("0000859796", "Japan Smaller Capitalization Fund"),
    "GAM": ("0000040417", "General American Investors Company Inc."),
    "BIGZ": ("0001836057", "BlackRock Innovation & Growth Trust"),
    "BMEZ": ("0001785971", "BlackRock Health Sciences Trust II"),
    "TTP": ("0001526329", "Tortoise Pipeline & Energy Fund Inc."),
}
logging.basicConfig(level=logging.INFO)

def _extract_filer_info_structured(xml_content: str, doc_url: str) -> dict:
    """Extracts filer name and ownership percent from XML"""
    try:
        tree = ET.fromstring(xml_content)

        # Find the first reportingOwner element (no namespace needed)
        owner = tree.find(".//reportingOwner")
        if owner is None:
            return {"filer_name": None, "ownership_percent": None, "source": "xml"}

        # Extract filer name
        name_elem = owner.find(".//rptOwnerName")
        filer_name = name_elem.text.strip() if name_elem is not None else "Unknown Filer"

        # Extract percent ownership
        percent_elem = owner.find(".//percentOfClass")
        ownership_percent = None
        if percent_elem is not None:
            try:
                ownership_percent = float(percent_elem.text.strip().replace("%", ""))
            except:
                ownership_percent = None

        return {
            "filer_name": filer_name,
            "ownership_percent": ownership_percent,
            "source": "xml"
        }

    except Exception as e:
        print(f"[ERROR] Failed to parse XML: {e}")
        return {"filer_name": None, "ownership_percent": None, "source": "xml_error"}



def _extract_filer_info_from_text(doc_text: str, doc_url: str) -> dict:
    if doc_text.strip().startswith("<?xml") or doc_url.endswith(".xml"):
        return _extract_filer_info_structured(doc_text, doc_url)

    soup = BeautifulSoup(doc_text, "html.parser")
    text = soup.get_text(separator="\n")

    if len(text) < 100:
        return {"filer_name": None, "ownership_percent": None, "source": "too_short"}

    name_match = re.search(r"(?i)NAME OF REPORTING PERSON.*?:?\s*(.*)", text)
    percent_match = re.search(r"(?i)Percent of class.*?:?\s*([\d.]+%)", text)

    return {
        "filer_name": name_match.group(1).strip() if name_match else None,
        "ownership_percent": percent_match.group(1).strip().strip('%') if percent_match else None,
        "source": "html"
    }

# ────────────────────────────────────────────────────────────────────────────────
# Dataclass
# ────────────────────────────────────────────────────────────────────────────────
@dataclass
class SECFiling:
    filing_id: str
    cik: str
    fund_name: str
    ticker: str
    filing_type: str
    filing_date: str
    acceptance_date: Optional[str]  
    accession_number: str
    filer_name: str
    ownership_percent: Optional[float]
    url: str
    is_activist: bool

    def to_dict(self):
        return asdict(self)

# ────────────────────────────────────────────────────────────────────────────────
# Main fetcher class
# ────────────────────────────────────────────────────────────────────────────────
class CEFSecFilingsFetcher:
    """Production-ready 13D/13G monitor for Closed-End Funds"""

    SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
    ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

    def __init__(
        self,
        ticker_map: Dict[str, Tuple[str, str]] = None,
        db_path: Path | str = "data/sec_filings.db",
    ):
        self.ticker_map = ticker_map or DEFAULT_TICKER_MAP
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.last_req_time = 0.0

        # SQLite cache
        Path(db_path).parent.mkdir(exist_ok=True, parents=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

        # logger
        self.log = logging.getLogger("cef.sec_filings")
        self.log.setLevel(logging.INFO)

    # ──────────────────── public entry point ────────────────────
    def fetch_cef_filings(self, days_back: int = 30) -> List[SECFiling]:
        """Fetch recent Schedule 13D/13G filings for all tickers in the map within the last `days_back` days."""
        self.log.info(f"Starting fetch for filings within the last {days_back} days")
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)
        all_filings: List[SECFiling] = []

        for ticker, (cik, fund_name) in self.ticker_map.items():
            self.log.info(f"⏳ {ticker} | Pulling submissions for CIK {cik}")
            data = self._get_submissions_json(cik)
            if not data:
                self.log.warning(f"No data returned for CIK {cik}")
                continue

            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            filing_dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            primaries = recent.get("primaryDocument", [])

            # Iterate through each filing and filter for recent Schedule 13D/13G
            for idx, form in enumerate(forms):
                form_name = form.upper() if form else ""
                if "13D" not in form_name and "13G" not in form_name:
                    # Skip forms that are not Schedule 13D or 13G
                    continue

                # Ensure corresponding data exists for this index
                if idx >= len(filing_dates) or idx >= len(accessions) or idx >= len(primaries):
                    self.log.warning(f"Skipping index {idx} for {ticker} due to mismatched data lengths.")
                    continue

                filing_date_str = filing_dates[idx]
                try:
                    filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                except Exception as e:
                    self.log.error(f"Could not parse filing date '{filing_date_str}' for {ticker}: {e}")
                    continue

                # Filter out filings older than the cutoff date
                if filing_date < cutoff_date:
                    self.log.info(f"Skipping {form} for {ticker} dated {filing_date_str} (older than {days_back} days).")
                    continue

                accession = accessions[idx]
                primary_doc = primaries[idx]
                filing_id = f"{cik}-{accession}"
                # Skip if this filing was already processed (cached)
                if self._exists(filing_id):
                    continue

                # Construct the URL for the primary document and download it
                url = f"{self.ARCHIVES_BASE}/{int(cik)}/{accession.replace('-', '')}/{primary_doc}"
                html = self._download_text(url)
                if not html:
                    self.log.warning(f"Failed to download document for {ticker} filing {accession}")
                    continue

                # Parse the downloaded document and create a SECFiling object
                filing = self._parse_document(
                    html=html,
                    meta={
                        "filing_id": filing_id,
                        "cik": cik,
                        "ticker": ticker,
                        "fund_name": fund_name,
                        "filing_type": form,
                        "filing_date": filing_date_str,
                        "acceptance_date": filing_date_str,
                        "accession": accession,
                        "url": url,
                    },
                )
                if filing:
                    all_filings.append(filing)
                    self._insert(filing)

        return all_filings
    # ──────────────────── network helpers ──────────────────
    def _rate_limit(self):
        delta = time.time() - self.last_req_time
        if delta < REQUEST_WAIT:
            time.sleep(REQUEST_WAIT - delta)
        self.last_req_time = time.time()

    def _get_submissions_json(self, cik: str):
        self._rate_limit()
        norm_cik = str(int(cik))  
        url = f"https://data.sec.gov/submissions/CIK{norm_cik.zfill(10)}.json"
        try:
            response = self.session.get(url, timeout=30)
            print(f"Request URL: {url}")
            print(f"Status Code: {response.status_code}")
            print(f"Response Length: {len(response.content)} bytes")
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: Received status code {response.status_code}")
                return None
        except Exception as e:
            print(f"Exception during request: {e}")
            return None


    def _download_text(self, url: str) -> Optional[str]:
        self._rate_limit()
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            self.log.warning("download failed %s: %s", url, e)
            return None

    # ──────────────────── HTML parsing ────────────────────
    FILER_RE = re.compile(r"(?i)NAME[S]?\s+OF\s+REPORTING\s+PERSON[S]?\s*(?:\(.*?\))?\s*:\s*([^\n\r]+)")
    PERCENT_RE = re.compile(r"(?i)Percent\s+of\s+Class\s+Represented\s+by\s+Amount\s*:\s*([\d\.]+)%")

    def _parse_document(self, html: str, meta: Dict) -> Optional[SECFiling]:
        try:
            # Use the new unified parsing logic to get filer info from HTML or XML
            info = _extract_filer_info_from_text(html, meta.get("url", ""))
            filer_name = info.get("filer_name") or "Unknown Filer"
            pct_str = info.get("ownership_percent")
            # Convert ownership percentage string to float (strip '%' if present)
            pct = None
            if pct_str:
                try:
                    pct = float(pct_str.strip("%"))
                except ValueError:
                    pct = None
            is_activist = self._is_activist(filer_name)

            return SECFiling(
                filing_id=meta["filing_id"],
                cik=meta["cik"],
                fund_name=meta["fund_name"],
                ticker=meta["ticker"],
                filing_type=meta["filing_type"],
                filing_date=meta["filing_date"],
                acceptance_date=meta.get("acceptance_date", meta["filing_date"]),
                accession_number=meta["accession"],
                filer_name=filer_name,
                ownership_percent=pct,
                url=meta["url"],
                is_activist=is_activist,
            )
        except Exception as e:
            print(f"[ERROR] Exception while parsing document: {e}")
            import traceback; traceback.print_exc()
            return None


    def close_connection(self):
        """Safely close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_connection()



    def _search(self, pattern, text, group=0, cast=None, default=None):
        try:
            # Use the compiled pattern's search; do not pass flags!
            match = pattern.search(text)
            if match:
                result = match.group(group).strip()
                print(f"[DEBUG] Pattern matched: {result[:50]}...")
                return cast(result) if cast else result
            else:
                print(f"[DEBUG] Pattern not found: {getattr(pattern, 'pattern', str(pattern))[:50]}...")
                return default
        except Exception as e:
            print(f"[ERROR] Regex search failed: {e}")
            return default



    def _is_activist(self, filer_name: str | None) -> bool:
        if not filer_name:
            return False
        lower = filer_name.lower()
        for _, aliases in ACTIVIST_ALIASES.items():
            if any(alias.lower() in lower for alias in aliases):
                return True
        return False

    # ──────────────────── SQLite persistence ────────────────────
    def _create_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS sec_filings (
                filing_id TEXT PRIMARY KEY,
                cik TEXT,
                fund_name TEXT,
                ticker TEXT,
                filing_type TEXT,
                filing_date TEXT,
                acceptance_date TEXT,
                accession_number TEXT,
                filer_name TEXT,
                ownership_percent REAL,
                url TEXT,
                is_activist BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        self.conn.commit()

    def _exists(self, filing_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM sec_filings WHERE filing_id=?", (filing_id,))
        return cur.fetchone() is not None


    def _insert(self, filing: SECFiling):
        """Insert a SECFiling instance into the sec_filings table, or ignore if duplicate. 
        Prints debug status and errors for full transparency.
        """
        try:
            print(f"[DEBUG] Inserting filing {filing.filing_id}")
            cur = self.conn.cursor()
            cur.execute("""INSERT OR IGNORE INTO sec_filings 
                (filing_id, cik, fund_name, ticker, filing_type, filing_date, acceptance_date,
                    accession_number, filer_name, ownership_percent, 
                    url, is_activist)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",  # ← 14 placeholders
                (
                    filing.filing_id,
                    filing.cik,
                    filing.fund_name,
                    filing.ticker,
                    filing.filing_type,
                    filing.filing_date,
                    filing.acceptance_date,
                    filing.accession_number,
                    filing.filer_name,
                    filing.ownership_percent,
                    filing.url,
                    int(filing.is_activist)
                )
            )

            self.conn.commit()
            print(f"[DEBUG] Successfully inserted {filing.filing_id}")
        except Exception as e:
            print(f"[ERROR] Database insert failed for {filing.filing_id}: {e}")
            raise


    def get_cached_filings(self, days_back: int = 30) -> List[SECFiling]:
        cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT filing_id, cik, fund_name, ticker, filing_type, filing_date,
                acceptance_date, accession_number, filer_name, ownership_percent,
                url, is_activist
            FROM sec_filings
            WHERE filing_date >= ?
            ORDER BY filing_date DESC
        ''', (cutoff_date,))
        rows = cursor.fetchall()
        filings = []
        for row in rows:
            filing = SECFiling(
                filing_id=row[0],
                cik=row[1],
                fund_name=row[2],
                ticker=row[3],
                filing_type=row[4],
                filing_date=row[5],
                acceptance_date=row[6],
                accession_number=row[7],
                filer_name=row[8],  
                ownership_percent=row[9],  
                url=row[12],
                is_activist=bool(row[13])
            )
            filings.append(filing)
        return filings
    def close_connection(self):
        """Safely close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.commit()  # Commit any pending transactions
                self.conn.close()
                self.conn = None
            except Exception as e:
                self.log.warning(f"Error closing connection: {e}")

    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_connection()

    def reconnect_if_needed(self):
        """Reconnect to database if connection is closed"""
        if not hasattr(self, 'conn') or self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._create_tables()

