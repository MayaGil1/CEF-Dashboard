from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration & logging
# ---------------------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class EnhancedNewsArticle:
    id: str
    title: str
    summary: str
    content: str
    url: str
    source: str
    published_at: str
    # --- enriched fields ---------------------------------------------------
    category: str = "general"
    priority_score: float = 1.0
    sentiment_score: float = 0.0
    relevance_score: float = 0.5
    tickers: List[str] = None
    fund_names: List[str] = None
    activist_mentions: List[str] = None

    def __post_init__(self) -> None:
        self.tickers = self.tickers or []
        self.fund_names = self.fund_names or []
        self.activist_mentions = self.activist_mentions or []

    def to_dict(self) -> Dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Lightweight rule-based classifier
# ---------------------------------------------------------------------------
class EnhancedCEFNewsClassifier:
    """Keyword classifier: fast, no third-party ML required."""

    def __init__(self) -> None:
        # Mapping full legal fund names ➜ primary ticker
        self.name_to_ticker: Dict[str, str] = {
            "PIMCO Dynamic Income Opportunities Fund": "PDO",
            "PIMCO Dynamic Income Fund": "PDI",
            "PIMCO High Income Fund": "PHK",
            "BlackRock Science & Tech Trust": "BST",
            "BlackRock Enhanced Equity Dividend Trust": "BDJ",
            "Nuveen Floating Rate Income Fund": "JFR",
            "Eaton Vance Tax-Advantaged Global Dividend Opportunities": "ETG",
            "Swiss Helvetia Fund": "SWZ",
            "Ellsworth Growth & Income Fund Ltd": "ECF",
            "Bancroft Fund Ltd": "BCV",
            "Neuberger Berman NextGen Connectivity Fund Inc.": "NBXG",
            "Japan Smaller Capitalization Fund": "JOF",
            "General American Investors Company Inc.": "GAM",
            "BlackRock Innovation & Growth Trust": "BIGZ",
            "BlackRock Health Sciences Trust II": "BMEZ",
            "Tortoise Pipeline & Energy Fund Inc.": "TTP",
        }
        # Cached lists for quick iteration
        self.cef_full_names: List[str] = list(self.name_to_ticker.keys())
        self.cef_tickers: List[str] = list(self.name_to_ticker.values())

        # Fund families / sponsors
        self.fund_families: List[str] = [
            "BlackRock",
            "Nuveen",
            "Eaton Vance",
            "PIMCO",
            "Calamos",
            "Cohen & Steers",
            "Gabelli",
            "Pioneer",
            "MFS",
            "Invesco",
            "Aberdeen",
            "Western Asset",
            "Neuberger Berman",
            "Tortoise",
            "Flaherty & Crumrine",
        ]

        # Generic CEF keywords
        self.cef_keywords: List[str] = [
            "closed-end fund",
            "closed end fund",
            "CEF",
            "discount to NAV",
            "premium to NAV",
            "net asset value",
            "fund distribution",
            "managed distribution",
            "rights offering",
            "tender offer",
            "liquidation",
            "conversion to open-end",
            "activist investor",
            "proxy contest",
            "board of directors",
            "fund merger",
            "fund reorganization",
            "distribution coverage",
            "leverage",
            "preferred shares",
            "interval fund", 
            "discount management program", 
            "new fund launch",
        ]

        # Activist firms that repeatedly target CEFs
        self.activist_firms: List[str] = [
            "Saba Capital",
            "Boaz Weinstein",
            "Bulldog Investors",
            "Karpus Investment Management",
            "City of London Investment Management",
            "Laxey Partners",
            "RiverNorth Capital",
            "Cornerstone Strategic Value Fund",
            "Engine Capital",
            "Ancora Advisors",
        ]

    # ---------------------------------------------------------------------
    # Single-article scoring
    # ---------------------------------------------------------------------
    def classify_article(
        self, title: str, content: str
    ) -> Tuple[str, float, float, List[str], List[str], List[str]]:
        """Return category, relevance, sentiment, tickers, fund-names, activists."""
        text = f"{title or ''} {content or ''}".lower()

        relevance = 0.0
        found_tickers: List[str] = []
        found_fund_names: List[str] = []
        found_activists: List[str] = []

        # --- full fund names (strongest) ----------------------------------
        for fullname in self.cef_full_names:
            if fullname.lower() in text:
                relevance += 0.6
                found_fund_names.append(fullname)
                found_tickers.append(self.name_to_ticker[fullname])

        # --- ticker match (medium) with ASA special case ------------------
        for tkr in self.cef_tickers:
            if re.search(rf"\b{re.escape(tkr.lower())}\b", text):
                # ignore Norwegian ASA company confusion
                if tkr == "ASA" and ("norway" in text or "norwegian" in text):
                    continue
                relevance += 0.3
                if tkr not in found_tickers:
                    found_tickers.append(tkr)

        # --- generic keywords (weak) -------------------------------------
        if any(kw in text for kw in self.cef_keywords):
            relevance += 0.1

        # --- activist firms ----------------------------------------------
        for firm in self.activist_firms:
            if firm.lower() in text:
                if firm == "Saba Capital":
                    relevance += 1.0  # Highest priority
                else:
                    relevance += 0.5
                found_activists.append(firm)
        relevance = min(relevance, 1.0)
        sentiment = 0.0  # placeholder – real model could replace this

        # Category heuristics
        category = "general"
        if any(w in text for w in ["activist", "proxy", "tender"]):
            category = "activist_activity"
        elif any(w in text for w in ["distribution", "dividend", "yield"]):
            category = "distributions"
        elif any(w in text for w in ["merger", "liquidation", "conversion"]):
            category = "corporate_actions"

        return category, relevance, sentiment, found_tickers, found_fund_names, found_activists


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------
class CEFNewsFetcher:
    """Pulls data from several cheap/free sources, dedups and classifies."""

    def __init__(self) -> None:
        self._setup_api_keys()
        self.classifier = EnhancedCEFNewsClassifier()
        self.article_cache: set[str] = set()
        self.session = requests.Session()
        self._setup_session()
        self._setup_feeds()

    # --------------------------------------------------------------------
    def _setup_api_keys(self):
        self.api_keys = {
            "newsapi": os.getenv("NEWSAPI_KEY"),
            "marketaux": os.getenv("MARKETAUX_API_KEY"),
            "alphavantage": os.getenv("ALPHA_VANTAGE_KEY"),
        }

    def _setup_session(self) -> None:
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/119.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

    def _setup_feeds(self):
        self.rss_feeds: Dict[str, str] = {
            # Direct CEF or asset-management press-release feeds
            "businesswire_financial": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
            "prnewswire_financial": "https://www.prnewswire.com/apac/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
            # Blogs & portals
            "etf_daily_news": "https://www.etfdailynews.com/feed/",
            "seeking_alpha_cef": "https://seekingalpha.com/tag/etf-portfolio-strategy.xml",
            "invezz_alternative": "https://invezz.com/news/alternative-investments/feed/",
            # Broader markets (we filter later)
            "marketwatch_funds": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
            "financial_planning": "https://www.financial-planning.com/feed?rss=true",
            # Example single-fund feed (BlackRock TCP Capital Corp.)
            "blackrock_news": "https://tcpcapital.com/rss/pressrelease.aspx",
        }

    # --------------------------------------------------------------------
    # Feed and webpage loaders
    # --------------------------------------------------------------------
    def _fetch_rss_feeds(self) -> List[Dict]:
        articles: List[Dict] = []
        for name, url in self.rss_feeds.items():
            try:
                logger.info("Fetching from %s", name)
                time.sleep(2)  # basic rate-limit
                feed = feedparser.parse(url)
                for entry in feed.entries[:20]:  # latest 20 each
                    art = self._parse_rss_entry(entry, name)
                    if art:
                        articles.append(art)
            except Exception as exc:
                logger.error("RSS error (%s): %s", name, exc)
        return articles

    def _parse_rss_entry(self, entry, source: str) -> Optional[Dict]:
        try:
            published_at = self._parse_date(entry) or datetime.utcnow()
            art_id = hashlib.md5(f"{entry.get('link','')}{entry.get('title','')}".encode()).hexdigest()
            if art_id in self.article_cache:
                return None
            self.article_cache.add(art_id)
            url = entry.get("link", "")
            if not url:
                return None
            content_value = (
                entry.get("content", [{}])[0].get("value", "")
                if entry.get("content")
                else entry.get("summary", "")
            )
            return {
                "id": art_id,
                "title": entry.get("title", ""),
                "summary": entry.get("summary", ""),
                "content": content_value,
                "url": url,
                "source": source,
                "published_at": published_at.isoformat(),
            }
        except Exception as exc:
            logger.error("Error parsing RSS: %s", exc)
            return None

    @staticmethod
    def _parse_date(entry) -> Optional[datetime]:
        date_fields = ["published", "updated", "pubDate", "published_parsed"]
        for fld in date_fields:
            if hasattr(entry, fld):
                try:
                    val = getattr(entry, fld)
                    if isinstance(val, str):
                        from dateutil import parser as dateparser

                        return dateparser.parse(val)
                    elif hasattr(val, "timetuple"):
                        return datetime(*val.timetuple()[:6])
                except Exception:
                    continue
        return None

    # --------------------------------------------------------------------
    # Minimal scraping for Seeking Alpha CEF landing page
    # --------------------------------------------------------------------
    def _scrape_seeking_alpha(self) -> List[Dict]:
        arts: List[Dict] = []
        try:
            url = "https://seekingalpha.com/etfs-and-funds/closed-end-funds"
            time.sleep(3)
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                return arts
            soup = BeautifulSoup(resp.content, "html.parser")
            links = soup.find_all("a", {"data-test-id": "post-list-item-title"})
            for a in links[:15]:
                href = urljoin(url, a.get("href"))
                title = a.get_text(strip=True)
                art_id = hashlib.md5(href.encode()).hexdigest()
                if art_id in self.article_cache:
                    continue
                self.article_cache.add(art_id)
                arts.append(
                    {
                        "id": art_id,
                        "title": title,
                        "summary": title,
                        "content": title,
                        "url": href,
                        "source": "seeking_alpha",
                        "published_at": datetime.utcnow().isoformat(),
                    }
                )
        except Exception as exc:
            logger.error("Seeking Alpha scrape error: %s", exc)
        return arts

    # --------------------------------------------------------------------
    # NewsAPI (optional, needs key)
    # --------------------------------------------------------------------
    def _fetch_newsapi(self) -> List[Dict]:
        if not self.api_keys.get("newsapi"):
            return []
        queries = [
            "\"closed-end fund\" OR \"closed end fund\"",
            "CEF discount OR \"premium to NAV\"",
            "activist investor fund",
            "fund distribution OR dividend",
            f"({' OR '.join(self.classifier.cef_tickers[:10])})",
            f"({' OR '.join(self.classifier.fund_families[:5])})",
        ]
        raw: List[Dict] = []
        for q in queries:
            try:
                time.sleep(2)
                url = "https://newsapi.org/v2/everything"
                params = {
                    "q": q,
                    "from": (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d"),
                    "sortBy": "publishedAt",
                    "language": "en",
                    "pageSize": 30,
                    "apiKey": self.api_keys["newsapi"],
                }
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    raw.extend(r.json().get("articles", []))
            except Exception as exc:
                logger.error("NewsAPI error (%s): %s", q, exc)
        out: List[Dict] = []
        for art in raw:
            if not art.get("title") or not art.get("url"):
                continue
            art_id = hashlib.md5(f"{art['url']}{art['title']}".encode()).hexdigest()
            if art_id in self.article_cache:
                continue
            self.article_cache.add(art_id)
            out.append(
                {
                    "id": art_id,
                    "title": art.get("title", ""),
                    "summary": art.get("description", ""),
                    "content": art.get("content", ""),
                    "url": art.get("url", ""),
                    "source": "newsapi",
                    "published_at": art.get("publishedAt", ""),
                }
            )
        return out

    # --------------------------------------------------------------------
    # Simple deduplication via Jaccard similarity on title+summary words
    # --------------------------------------------------------------------
    @staticmethod
    def _deduplicate(articles: List[Dict]) -> List[Dict]:
        uniques: List[Dict] = []
        fingerprints: List[frozenset] = []
        for art in articles:
            text = f"{art.get('title','')} {art.get('summary','')}".lower()
            words = frozenset(re.sub(r"[^\w\s]", "", text).split())
            if not words:
                continue
            duplicate = False
            for fp in fingerprints:
                inter = len(words & fp)
                union = len(words | fp)
                if union and inter / union > 0.7:
                    duplicate = True
                    break
            if not duplicate:
                fingerprints.append(words)
                uniques.append(art)
        return uniques

    # --------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------
    def fetch_all_news(self) -> List[EnhancedNewsArticle]:
        logger.info("Starting news fetch cycle")
        self.article_cache.clear()

        raw: List[Dict] = []
        raw.extend(self._fetch_rss_feeds())
        raw.extend(self._scrape_seeking_alpha())
        raw.extend(self._fetch_newsapi())
        logger.info("Collected %d raw articles", len(raw))

        raw = self._deduplicate(raw)
        logger.info("%d unique articles after dedup", len(raw))

        processed: List[EnhancedNewsArticle] = []
        for art in raw:
            try:
                cat, rel, sent, tickers, fund_names, activists = self.classifier.classify_article(
                    art.get("title", ""), art.get("content", art.get("summary", ""))
                )
                obj = EnhancedNewsArticle(
                    id=art["id"],
                    title=art["title"],
                    summary=art.get("summary", ""),
                    content=art.get("content", ""),
                    url=art["url"],
                    source=art["source"],
                    published_at=art["published_at"],
                    category=cat,
                    priority_score=rel * 5,
                    sentiment_score=sent,
                    relevance_score=rel,
                    tickers=tickers,
                    fund_names=fund_names,
                    activist_mentions=activists,
                )
                # keep almost everything (relevance>0.05 or explicit signals)
                if obj.relevance_score > 0.05 or obj.tickers or obj.activist_mentions:
                    processed.append(obj)
            except Exception as exc:
                logger.error("Processing error: %s", exc)

        processed.sort(key=lambda x: x.priority_score, reverse=True)
        logger.info("Returning %d relevant articles", len(processed))
        return processed


# Alias for backward compatibility with old import name
RobustCEFNewsFetcher = CEFNewsFetcher


# ---------------------------------------------------------------------------
# Quick debug
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    fetcher = CEFNewsFetcher()
    articles = fetcher.fetch_all_news()
    print(f"Total relevant: {len(articles)}")
    for art in articles[:5]:
        print("-", art.title, "|", art.tickers, "|", art.category)

def get_news_data():
    """Helper function to get news data as a pandas DataFrame for Streamlit display"""
    import pandas as pd
    
    try:
        fetcher = CEFNewsFetcher()
        articles = fetcher.fetch_all_news()
        
        # Convert to DataFrame for Streamlit display
        data = []
        for article in articles:
            data.append({
                'Title': article.title,
                'Category': article.category,
                'Published': article.published_at,
                'Tickers': ', '.join(article.tickers) if article.tickers else 'N/A',
                'Source': article.source,
                'Article': article.url,
                'Priority': article.priority_score,
                'Relevance': article.relevance_score
            })
        
        return pd.DataFrame(data)
    
    except Exception as e:
        logger.error(f"Error fetching news data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
