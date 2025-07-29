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
from lxml import etree 

_XML_RECOVER = etree.XMLParser(recover=True)

def _safe_xml_root(text: str) -> etree._Element | None:
    """Return lxml root even if the Schedule 13D/G XML is malformed."""
    # fix stray &
    text = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', text)
    try:
        return etree.fromstring(text.encode(), parser=_XML_RECOVER)
    except Exception:
        return None

def _issuer_from_root(root: etree._Element) -> tuple[str, str]:
    """
    Extract (ticker, fund_name) from XML filings using issuer-related tags.
    Returns ('','') if nothing found.
    """
    # Try standard <issuer> block first
    node = root.find(".//issuer")
    if node is not None:
        return (
            (node.findtext(".//tradingSymbol") or "").strip(),
            (node.findtext(".//nameOfIssuer")   or "").strip(),
        )

    # Fallback: check for alternate issuer fields directly
    ticker = (root.findtext(".//tradingSymbol") or "").strip()
    name = (
        (root.findtext(".//nameOfIssuer") or "") or
        (root.findtext(".//issuerName")   or "") or
        (root.findtext(".//subjectCompany//name") or "")
    ).strip()

    return ticker, name

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
    "SCHEDULE 13D", "SCHEDULE 13D/A", 
    "SCHEDULE 13G", "SCHEDULE 13G/A",
}

ACTIVIST_CIKS = {                 
    "0001510281": "Saba Capital Management",
    "0001048703": "Karpus Investment Management",
    "0001504304": "Bulldog Investors",
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
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s",)

def _extract_filer_from_xml_text(xml_content: str) -> dict:
    """Extract filer from XML content using text patterns"""
    # Convert to text and look for reporting person patterns
    soup = BeautifulSoup(xml_content, 'html.parser')
    text = soup.get_text(separator='\n')
    
    # Patterns specific to XML-based Schedule 13D/13G filings
    xml_filer_patterns = [
        r'(?i)name\s+of\s+reporting\s+person[s]?\s*\n\s*([^\n\r]+)',
        r'(?i)names\s+of\s+reporting\s+persons\s*\n\s*([^\n\r]+)',
        r'(?i)reporting\s+person[s]?\s*:\s*([^\n\r]+)',
        r'(?i)<name[^>]*>([^<]+)</name>',
        r'(?i)filer[^>]*>([^<\n]+)',
    ]
    
    for pattern in xml_filer_patterns:
        try:
            match = re.search(pattern, text, re.MULTILINE)
            if match:
                candidate_name = match.group(1).strip()
                # Clean and validate the name
                if len(candidate_name) > 3 and not candidate_name.isdigit():
                    # Skip obvious non-names
                    if not any(skip_word in candidate_name.lower() for skip_word in 
                             ['item', 'comment', 'amend', 'securities', 'schedule', 'cusip', 'check']):
                        return {
                            "filer_name": candidate_name,
                            "source": "xml_text_fallback"
                        }
        except Exception:
            continue
    
    return {"filer_name": None, "source": "xml_text_failed"}

def _extract_filer_info_structured(xml_content: str, doc_url: str) -> dict:
    """Enhanced XML parsing for post-December 2024 Schedule 13D/13G filings"""
    try:
        # Clean common XML issues before parsing
        cleaned_xml = xml_content
        
        # Fix common malformed XML issues
        cleaned_xml = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', cleaned_xml)
        
        root = _safe_xml_root(cleaned_xml)
        for elem in root.iter():
            if elem.tag.startswith('{'):
                elem.tag = elem.tag.split('}', 1)[1]
          
        if root is None:                         
            return _extract_filer_from_xml_text(xml_content)
        
        # Look for reporting person elements - try different possible tag names
        reporting_person_tags = [
            './/reportingOwner', './/reportingPerson', './/filerName', 
            './/personName', './/entityName'
        ]
        
        for tag in reporting_person_tags:
            elements = root.findall(tag)
            for elem in elements:
                # Look for name within the element
                name_tags = ['.//name', './/rptOwnerName', './/entityName', './/personName', './/reportingPersonName']
                for name_tag in name_tags:
                    name_elem = elem.find(name_tag)
                    if name_elem is not None and name_elem.text:
                        filer_name = name_elem.text.strip()
                        if len(filer_name) > 3:  # Basic validation
                            return {
                                "filer_name": filer_name,
                                "source": "xml_enhanced"
                            }
        
        # If no structured elements found, try text-based extraction on XML
        return _extract_filer_from_xml_text(xml_content)
        
    except ET.ParseError as e:
        print(f"[ERROR] Failed to parse XML: {e}")
        # Fall back to text parsing
        return _extract_filer_from_xml_text(xml_content)
    except Exception as e:
        print(f"[ERROR] XML parsing exception: {e}")
        return {"filer_name": None, "source": "xml_error"}

TICKER_RE = re.compile(
    r"(?i)(trading\\s*symbol[s]?|security\\s*symbol|ticker)([^A-Z]{0,20})([A-Z]{1,5})"
)
ISSUER_RE = re.compile(
    r"(?i)(name\\s*of\\s*issuer|issuer\\s*name)([^\\w]{0,40})([\\w .,&’\\-]{4,120})"
)

def _quick_html_issuer(html_text: str) -> tuple[str, str]:
    """
    Lightweight pattern search for ticker and fund name inside raw HTML
    returned by legacy Schedule 13D/13G filings.
    Returns (ticker, fund_name); empty strings if not found.
    """
    ticker, fund = "", ""
    for line in html_text.splitlines():
        if not line.strip():
            continue
        m = TICKER_RE.search(line)
        if m:
            ticker = m.group(3).strip().upper()
        m2 = ISSUER_RE.search(line)
        if m2:
            fund = m2.group(3).strip()
        if ticker or fund:
            break
    return ticker, fund

def _extract_filer_info_from_text(doc_text: str, doc_url: str) -> dict:
    """Simplified HTML parsing for legacy Schedule 13D/13G filings"""
    
    # First check if this is XML format
    if doc_text.strip().startswith("<?xml") or "xslSCHEDULE" in doc_url:
        return _extract_filer_info_structured(doc_text, doc_url)
    
    # Parse HTML content
    soup = BeautifulSoup(doc_text, "html.parser")
    text = soup.get_text(separator="\n")
    
    # Simple approach: look for "Name of reporting person" followed by the name
    lines = text.split('\n')
    
    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        
        # Find the "Name of reporting person" line
        if 'name of reporting person' in line_lower or 'names of reporting persons' in line_lower:
            # Look in the next few lines for the actual name
            for j in range(i + 1, min(i + 5, len(lines))):
                candidate = lines[j].strip()
                
                # Skip empty lines and obvious non-names
                if not candidate or len(candidate) < 4:
                    continue
                    
                # Skip lines that are clearly not entity names
                if any(skip in candidate.lower() for skip in 
                      ['check', 'sec use', 'source', 'cusip', 'telephone', 
                       'address', 'item', 'comment', 'amendment', 'percentage used']):
                    continue
                
                # If it looks like a valid entity name, return it
                if not candidate.isdigit() and any(c.isalpha() for c in candidate):
                    return {
                        "filer_name": candidate,
                        "source": "html_simple"
                    }
    
    # Fallback: look for known activist investor names in the text
    known_activists = [
        'saba capital', 'karpus', 'starboard', 'elliott', 'bulldog', 
        'valueact', 'pershing', 'trian', 'third point'
    ]
    
    for line in lines:
        line_lower = line.lower()
        for activist in known_activists:
            if activist in line_lower and len(line.strip()) > 5:
                return {
                    "filer_name": line.strip(),
                    "source": "activist_fallback"
                }
    
    return {"filer_name": None, "source": "html_failed"}

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
        activist_ciks: Dict[str, str] | None = None,
        db_path: Path | str = "data/sec_filings.db",
    ):
        self.ticker_map = ticker_map or DEFAULT_TICKER_MAP
        self.activist_ciks = activist_ciks or ACTIVIST_CIKS
        self.db_path = db_path
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
        self.log.info("▶ Fetching filings ≤ %s days old", days_back)
        cutoff = datetime.utcnow() - timedelta(days=days_back)
        all_filings: List[SECFiling] = []
        all_filings += self._fetch_by_cef_tickers(cutoff)
        all_filings += self._fetch_by_activist_ciks(cutoff)
        return all_filings
    
    def _fetch_by_cef_tickers(self, cutoff: datetime) -> List[SECFiling]:
        filings: List[SECFiling] = []
        for ticker, (cik, fund_name) in self.ticker_map.items():
            self.log.info(f"⏳ {ticker}  | CIK {cik}")
            data = self._get_submissions_json(cik)
            if not data:
                continue

            recent    = data.get("filings", {}).get("recent", {})
            forms     = recent.get("form", [])
            dates     = recent.get("filingDate", [])
            accs      = recent.get("accessionNumber", [])
            primaries = recent.get("primaryDocument", [])

            for idx, form in enumerate(forms):
                if form.upper() not in TARGET_FORMS:
                    continue
                if idx >= len(dates) or idx >= len(accs) or idx >= len(primaries):
                    continue
                fdate = datetime.strptime(dates[idx], "%Y-%m-%d")
                if fdate < cutoff:
                    continue

                filing = self._download_and_parse(
                    cik        = cik,
                    accession  = accs[idx],
                    primary    = primaries[idx],
                    fund_name  = fund_name,
                    ticker     = ticker,
                    filing_type= form,
                    filing_date= dates[idx],
                )
                if filing:
                    filings.append(filing)
        return filings

    # ───────── Activist-CIK path (NEW) ─────────
    def _fetch_by_activist_ciks(self, cutoff: datetime) -> List[SECFiling]:
        """
        Pull recent 13D/13G filings where *the filer itself* is Saba/Karpus/Bulldog.
        """
        filings: List[SECFiling] = []
        for cik, friendly in self.activist_ciks.items():
            self.log.info(f"⏳ Activist {friendly} (CIK {cik})")
            data = self._get_submissions_json(cik)
            if not data:
                continue

            recent    = data.get("filings", {}).get("recent", {})
            forms     = recent.get("form", [])
            dates     = recent.get("filingDate", [])
            accs      = recent.get("accessionNumber", [])
            primaries = recent.get("primaryDocument", [])

            if not recent or not any([forms, dates, accs, primaries]):
                continue

            for idx, form in enumerate(forms):
                if form.upper() not in TARGET_FORMS:
                    continue
                if idx >= len(dates) or idx >= len(accs) or idx >= len(primaries):
                    continue
                fdate = datetime.strptime(dates[idx], "%Y-%m-%d")
                if fdate < cutoff:
                    continue

                # Unknown ticker/issuer at this stage; leave blank —
                # the parsing step may recover it from the document text.
                filing = self._download_and_parse(
                    cik        = cik,
                    accession  = accs[idx],
                    primary    = primaries[idx],
                    fund_name  = "N/A",
                    ticker     = "",
                    filing_type= form,
                    filing_date= dates[idx],
                )
                if filing:
                    filings.append(filing)
        return filings
    def _download_and_parse(
        self,
        *,
        cik: str,
        accession: str,
        primary: str,
        fund_name: str,
        ticker: str,
        filing_type: str,
        filing_date: str,
    ) -> Optional[SECFiling]:
        filing_id = f"{cik}-{accession}"
        if self._exists(filing_id):
            return None

        url  = f"{self.ARCHIVES_BASE}/{int(cik)}/{accession.replace('-', '')}/{primary}"
        html = self._download_text(url)
        if not html:
            return None

        filing = self._parse_document(
            html,
            meta = {
                "filing_id":     filing_id,
                "cik":           cik,
                "ticker":        ticker,
                "fund_name":     fund_name,
                "filing_type":   filing_type,
                "filing_date":   filing_date,
                "acceptance_date": filing_date,
                "accession":     accession,
                "url":           url,
            },
        )
        
        if filing and not filing.ticker:
            js = self._get_submissions_json(filing.cik)
            if js and js.get("tickers"):
                filing.ticker = js["tickers"][0]
                filing.fund_name = js.get("name", filing.fund_name or "N/A")

        # Extract issuer from XML as a last resort
        if filing and (not filing.ticker or filing.fund_name in ("N/A", "")):
            root = _safe_xml_root(html)
            if root is not None:
                tkr, name = _issuer_from_root(root)
                if tkr:
                    filing.ticker = tkr
                if name:
                    filing.fund_name = name
        if filing and (not filing.ticker or filing.fund_name in ("", "N/A")):      
            m = re.search(r"(?i)name of issuer[^A-Za-z0-9]*([\w .,&-]{4,})", html) 
            if m:                                                                  
                filing.fund_name = filing.fund_name or m.group(1).strip()          
            m = re.search(r"(?i)trading symbol[^A-Z]*([A-Z]{2,5})", html)          
            if m:                                                                
                filing.ticker = filing.ticker or m.group(1).strip()              

        if filing:
            self._insert(filing)
        return filing

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
            return None

    # ──────────────────── HTML parsing ────────────────────
    FILER_RE = re.compile(r"(?i)NAME[S]?\s+OF\s+REPORTING\s+PERSON[S]?\s*(?:\(.*?\))?\s*:\s*([^\n\r]+)")

    def _parse_document(self, html: str, meta: Dict) -> Optional[SECFiling]:
        try:
            # Use the updated parsing logic
            info = _extract_filer_info_from_text(html, meta.get("url", ""))
            filer_name = info.get("filer_name") or "Unknown Filer"
            parsing_source = info.get("source", "unknown")
            
            ticker = meta.get("ticker", "") or ""
            fund_name = meta.get("fund_name", "") or ""
            if not ticker or not fund_name or ticker in ("", "N/A") or fund_name in ("", "N/A"):
                tkr, fund = _quick_html_issuer(html)
                ticker = ticker or tkr
                if not fund_name or fund_name in ("", "N/A"):
                    fund_name = fund
            # Save back to meta for use in SECFiling creation
            meta["ticker"] = ticker or ""
            meta["fund_name"] = fund_name or ""
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
                url=meta["url"],
                is_activist=is_activist,
            )
            
        except Exception as e:
            print(f"[ERROR] Exception while parsing document {meta.get('filing_id', 'unknown')}: {e}")
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
                return cast(result) if cast else result
            else:
                return default
        except Exception as e:
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
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""INSERT OR IGNORE INTO sec_filings 
                (filing_id, cik, fund_name, ticker, filing_type, filing_date, acceptance_date,
                    accession_number, filer_name, 
                    url, is_activist)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",  
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
                    filing.url,
                    int(filing.is_activist)
                )
            )

            self.conn.commit()
        except Exception as e:
            raise


    def get_cached_filings(self, days_back: int = 30) -> List[SECFiling]:
        cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT filing_id, cik, fund_name, ticker, filing_type, filing_date,
                acceptance_date, accession_number, filer_name,
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
                url=row[9],
                is_activist=bool(row[10])
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
                if hasattr(self, 'log'):
                    self.log.warning(f"Error closing connection: {e}")

    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_connection()

    def reconnect_if_needed(self):
        """Reconnect to database if connection is closed"""
        if not hasattr(self, 'conn') or self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False) 
            self._create_tables()

