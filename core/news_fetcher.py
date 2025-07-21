import os
import requests
import feedparser
import time
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import hashlib
import logging
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class EnhancedNewsArticle:
    id: str
    title: str
    summary: str
    content: str
    url: str
    source: str
    published_at: str
    category: str = "general"
    priority_score: float = 1.0
    sentiment_score: float = 0.0
    relevance_score: float = 0.5
    tickers: List[str] = None
    fund_names: List[str] = None
    activist_mentions: List[str] = None
    
    def __post_init__(self):
        if self.tickers is None:
            self.tickers = []
        if self.fund_names is None:
            self.fund_names = []
        if self.activist_mentions is None:
            self.activist_mentions = []

    def to_dict(self) -> Dict:
        """Convert to dictionary for frontend compatibility"""
        return asdict(self)

class EnhancedCEFNewsClassifier:
    def __init__(self):
        # Expanded CEF tickers and fund families
        self.cef_tickers = [
            'ASA', 'SWZ', 'ECF', 'BCV', 'NBXG', 'JOF', 'GAM', 'BIGZ', 'BMEZ', 'TTP',
            'PHD', 'PHT', 'MAV', 'MHI', 'MIO', 'HNW', 'RCS', 'MCI', 'RSF', 'UTF',
            'EVV', 'EXG', 'AVK', 'CHW', 'CII', 'EOI', 'ETW', 'EVG', 'EVN', 'EVT'
        ]
        
        # Fund management companies and families
        self.fund_families = [
            'BlackRock', 'Nuveen', 'Eaton Vance', 'PIMCO', 'Calamos', 'Cohen & Steers',
            'Gabelli', 'Pioneer', 'MFS', 'Invesco', 'Aberdeen', 'Western Asset',
            'Neuberger Berman', 'Tortoise', 'Flaherty & Crumrine'
        ]
        
        # Enhanced keyword list
        self.cef_keywords = [
            'closed-end fund', 'closed end fund', 'CEF', 'discount to NAV', 'premium to NAV',
            'net asset value', 'fund distribution', 'managed distribution', 'rights offering',
            'tender offer', 'liquidation', 'conversion to open-end', 'activist investor',
            'proxy contest', 'board of directors', 'fund merger', 'fund reorganization',
            'distribution coverage', 'leverage', 'preferred shares'
        ]
        
        # Activist firms known for CEF activity
        self.activist_firms = [
            'Saba Capital', 'Bulldog Investors', 'Karpus Investment Management',
            'City of London Investment Management', 'Laxey Partners', 'RiverNorth Capital',
            'Cornerstone Strategic Value Fund', 'Engine Capital', 'Ancora Advisors'
        ]

class CEFNewsFetcher:  # Changed name to match import
    def __init__(self):
        self.setup_configuration()
        self.classifier = EnhancedCEFNewsClassifier()
        self.article_cache = set()
        self.session = requests.Session()
        self.setup_session_headers()
        self.setup_rss_feeds()
        
    def setup_configuration(self):
        """Setup API keys and basic configuration"""
        self.api_keys = {
            'newsapi': os.getenv('NEWSAPI_KEY'),
            'marketaux': os.getenv('MARKETAUX_API_KEY'),
            'alphavantage': os.getenv('ALPHA_VANTAGE_KEY')
        }
        
    def setup_session_headers(self):
        """Setup realistic browser headers for web scraping"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def setup_rss_feeds(self):
        """Enhanced RSS feed sources specifically for CEF content"""
        self.rss_feeds = {
            # Business Wire - Financial Services
            'businesswire_financial': 'https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==',
            
            # PR Newswire - Financial Services & Investing
            'prnewswire_financial': 'https://www.prnewswire.com/apac/rss/financial-services-latest-news/financial-services-latest-news-list.rss',
            
            # ETF Daily News
            'etf_daily_news': 'https://www.etfdailynews.com/feed/',
            
            # Seeking Alpha - CEF Articles
            'seeking_alpha_cef': 'https://seekingalpha.com/tag/etf-portfolio-strategy.xml',
            
            # Invezz Alternative Investments
            'invezz_alternative': 'https://invezz.com/news/alternative-investments/feed/',
            
            # MarketWatch - Mutual Funds & ETFs
            'marketwatch_funds': 'https://feeds.content.dowjones.io/public/rss/mw_topstories',
            
            # Financial Planning Magazine
            'financial_planning': 'https://www.financial-planning.com/feed?rss=true',
            
            # Fund-specific RSS feeds
            'blackrock_news': 'https://tcpcapital.com/rss/pressrelease.aspx',
        }

    def fetch_rss_feeds(self) -> List[Dict]:
        """Fetch articles from all RSS sources with rate limiting"""
        articles = []
        
        for feed_name, feed_url in self.rss_feeds.items():
            try:
                logger.info(f"Fetching from {feed_name}...")
                
                # Add delay to respect rate limits
                time.sleep(2)
                
                feed = feedparser.parse(feed_url)
                
                for entry in feed.entries[:10]:  # Limit to recent articles
                    article = self.parse_rss_entry(entry, feed_name)
                    if article:
                        articles.append(article)
                        
            except Exception as e:
                logger.error(f"Error fetching RSS from {feed_name}: {e}")
                continue
                
        return articles

    def parse_rss_entry(self, entry, source: str) -> Optional[Dict]:
        """Parse individual RSS entry into article dictionary"""
        try:
            # Extract publication date
            published_at = self._parse_date(entry)
            if not published_at:
                published_at = datetime.now()
            
            # Generate unique ID
            article_id = hashlib.md5(
                f"{entry.get('link', '')}{entry.get('title', '')}".encode()
            ).hexdigest()
            
            # Check for duplicates
            if article_id in self.article_cache:
                return None
            
            self.article_cache.add(article_id)
            
            # Get URL
            url = entry.get('link', '')
            if not url:
                return None
            
            article_data = {
                'id': article_id,
                'title': entry.get('title', '') or '',
                'summary': entry.get('summary', '') or '',
                'content': (
                    (entry.get('content', [{}])[0].get('value', '') if entry.get('content') else entry.get('summary', ''))
                    or ''
                ),
                'url': url,
                'source': source,
                'published_at': published_at.isoformat()
            }

            
            return article_data
            
        except Exception as e:
            logger.error(f"Error parsing RSS entry: {e}")
            return None

    def _parse_date(self, entry) -> Optional[datetime]:
        """Parse publication date from RSS entry"""
        date_fields = ['published', 'updated', 'pubDate', 'published_parsed']
        
        for field in date_fields:
            if hasattr(entry, field):
                try:
                    date_value = getattr(entry, field)
                    if isinstance(date_value, str):
                        from dateutil import parser
                        return parser.parse(date_value)
                    elif hasattr(date_value, 'timetuple'):
                        return datetime(*date_value.timetuple()[:6])
                except:
                    continue
        
        return None

    def scrape_seeking_alpha_cef(self) -> List[Dict]:
        """Scrape CEF-related articles from Seeking Alpha"""
        articles = []
        
        try:
            # Seeking Alpha CEF section
            url = "https://seekingalpha.com/etfs-and-funds/closed-end-funds"
            
            time.sleep(3)  # Rate limiting
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find article links (this selector may need adjustment)
                article_links = soup.find_all('a', {'data-test-id': 'post-list-item-title'})
                
                for link in article_links[:5]:  # Limit to recent articles
                    article_url = urljoin(url, link.get('href'))
                    title = link.get_text(strip=True)
                    
                    # Create article object
                    article_id = hashlib.md5(article_url.encode()).hexdigest()
                    
                    if article_id not in self.article_cache:
                        self.article_cache.add(article_id)
                        
                        articles.append({
                            'id': article_id,
                            'title': title,
                            'url': article_url,
                            'source': 'seeking_alpha',
                            'published_at': datetime.now().isoformat(),
                            'summary': title,
                            'content': title
                        })
                        
        except Exception as e:
            logger.error(f"Error scraping Seeking Alpha: {e}")
            
        return articles

    def scrape_cefconnect(self) -> List[Dict]:
        """Scrape news and updates from CEFConnect"""
        articles = []
        
        try:
            url = "https://www.cefconnect.com/news"
            
            time.sleep(3)  # Rate limiting
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find news items (selector may need adjustment based on actual site structure)
                news_items = soup.find_all('div', class_='news-item')
                
                for item in news_items[:10]:
                    title_elem = item.find('h3') or item.find('h2') or item.find('a')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        link = title_elem.get('href') if title_elem.name == 'a' else None
                        
                        if link:
                            article_url = urljoin(url, link)
                            article_id = hashlib.md5(article_url.encode()).hexdigest()
                            
                            if article_id not in self.article_cache:
                                self.article_cache.add(article_id)
                                
                                articles.append({
                                    'id': article_id,
                                    'title': title,
                                    'url': article_url,
                                    'source': 'cefconnect',
                                    'published_at': datetime.now().isoformat(),
                                    'summary': title,
                                    'content': title
                                })
                                
        except Exception as e:
            logger.error(f"Error scraping CEFConnect: {e}")
            
        return articles

    def enhanced_classify_article(self, title: str, content: str) -> tuple:
        """More sophisticated classification with lower thresholds"""
        title   = title or ''
        content = content or ''
        text    = f"{title} {content}".lower()
        relevance_score = 0.0
        found_tickers = []
        found_fund_names = []
        found_activists = []
        
        # Check for CEF tickers (higher weight)
        for ticker in self.classifier.cef_tickers:
            if re.search(r'\b' + re.escape(ticker.lower()) + r'\b', text):
                relevance_score += 0.4
                found_tickers.append(ticker)
        
        # Check for fund families
        for family in self.classifier.fund_families:
            if family.lower() in text:
                relevance_score += 0.3
                found_fund_names.append(family)
        
        # Check for CEF keywords
        for keyword in self.classifier.cef_keywords:
            if keyword.lower() in text:
                relevance_score += 0.2
        
        # Check for activist firms
        for activist in self.classifier.activist_firms:
            if activist.lower() in text:
                relevance_score += 0.5
                found_activists.append(activist)
        
        # Bonus for multiple indicators
        if len(found_tickers) > 1:
            relevance_score += 0.2
        
        # Determine category
        category = 'general'
        if any(term in text for term in ['activist', 'proxy', 'tender']):
            category = 'activist_activity'
        elif any(term in text for term in ['distribution', 'dividend', 'yield']):
            category = 'distributions'
        elif any(term in text for term in ['merger', 'liquidation', 'conversion']):
            category = 'corporate_actions'
        
        # Lower threshold for acceptance (was 0.1, now 0.05)
        final_relevance = min(relevance_score, 1.0)
        
        return category, final_relevance, 0.0, found_tickers, found_activists

    def fetch_enhanced_newsapi(self) -> List[Dict]:
        """Enhanced NewsAPI queries with multiple search terms"""
        if not self.api_keys.get('newsapi'):
            return []
        
        articles = []
        
        # Multiple targeted queries
        queries = [
            'closed-end fund OR "closed end fund"',
            'CEF discount OR "premium to NAV"',
            'activist investor fund',
            'fund distribution OR dividend',
            f"({' OR '.join(self.classifier.cef_tickers[:10])})",  # Top tickers
            f"({' OR '.join(self.classifier.fund_families[:5])})"  # Top fund families
        ]
        
        for query in queries:
            try:
                time.sleep(2)  # Rate limiting
                
                url = 'https://newsapi.org/v2/everything'
                params = {
                    'q': query,
                    'from': (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d'),
                    'sortBy': 'publishedAt',
                    'language': 'en',
                    'apiKey': self.api_keys['newsapi'],
                    'pageSize': 20
                }
                
                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    articles.extend(data.get('articles', []))
                    
            except Exception as e:
                logger.error(f"Error with NewsAPI query '{query}': {e}")
                continue
        
        return self.process_newsapi_articles(articles)

    def process_newsapi_articles(self, articles: List[Dict]) -> List[Dict]:
        """Process NewsAPI articles into standard format"""
        processed = []
        
        for article in articles:
            if not article.get('title') or not article.get('url'):
                continue
                
            article_id = hashlib.md5(
                f"{article.get('url', '')}{article.get('title', '')}".encode()
            ).hexdigest()
            
            if article_id in self.article_cache:
                continue
            
            self.article_cache.add(article_id)
            
            processed_article = {
                'id': article_id,
                'title': article.get('title', ''),
                'summary': article.get('description', ''),
                'content': article.get('content', ''),
                'url': article.get('url', ''),
                'source': 'newsapi',
                'published_at': article.get('publishedAt', '')
            }
            
            processed.append(processed_article)
        
        return processed

    def advanced_deduplication(self, articles: List[Dict]) -> List[Dict]:
        unique_articles = []
        seen_content = set()
        
        for article in articles:
            # Create content fingerprint
            title = article.get('title', '') or ''
            summary = article.get('summary', '') or ''
            content_text = f"{title} {summary}".lower()
            content_text = re.sub(r'[^\w\s]', '', content_text)  # Remove punctuation
            content_words = frozenset(content_text.split())  # Use frozenset
            
            # Check for similar content
            is_duplicate = False
            for seen in seen_content:
                # Calculate Jaccard similarity
                intersection = len(content_words.intersection(seen))
                union = len(content_words.union(seen))
                similarity = intersection / union if union > 0 else 0
                
                if similarity > 0.7:  # 70% similarity threshold
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                seen_content.add(content_words)
                unique_articles.append(article)
        
        return unique_articles


    def process_articles(self, raw_articles: List[Dict]) -> List[EnhancedNewsArticle]:
        """Process raw articles through classification pipeline"""
        processed_articles = []
        
        for raw_article in raw_articles:
            try:
                # Skip if missing essential fields
                if not all(key in raw_article for key in ['title', 'url', 'published_at']):
                    continue
                
                # Apply classification
                category, relevance_score, sentiment_score, tickers, activists = \
                    self.enhanced_classify_article(
                        raw_article.get('title', '') or '', 
                        raw_article.get('content', raw_article.get('summary', '')) or ''
                    )

                
                # Create NewsArticle object
                article = EnhancedNewsArticle(
                    id=raw_article['id'],
                    title=raw_article['title'],
                    summary=raw_article.get('summary', ''),
                    content=raw_article.get('content', ''),
                    url=raw_article['url'],
                    source=raw_article['source'],
                    published_at=raw_article['published_at'],
                    category=category,
                    priority_score=relevance_score * 5,  # Scale to 0-5
                    sentiment_score=sentiment_score,
                    relevance_score=relevance_score,
                    tickers=tickers,
                    fund_names=[],  # Will be populated by classification
                    activist_mentions=activists
                )
                
                # Only keep relevant articles
                if article.relevance_score > 0.05 or article.tickers or article.activist_mentions:
                    processed_articles.append(article)
                    
            except Exception as e:
                logger.error(f"Error processing article: {e}")
                continue
        
        return processed_articles

    def fetch_all_news(self) -> List[EnhancedNewsArticle]:
        """Main method to fetch and process all news"""
        self.article_cache = set()
        logger.info("Starting enhanced news fetch cycle")
        
        all_articles = []
        
        # 1. Fetch from RSS feeds
        try:
            rss_articles = self.fetch_rss_feeds()
            all_articles.extend(rss_articles)
            logger.info(f"Fetched {len(rss_articles)} RSS articles")
        except Exception as e:
            logger.error(f"Error fetching RSS articles: {e}")
        
        # 2. Scrape specialized sources
        try:
            sa_articles = self.scrape_seeking_alpha_cef()
            all_articles.extend(sa_articles)
            logger.info(f"Fetched {len(sa_articles)} Seeking Alpha articles")
        except Exception as e:
            logger.error(f"Error scraping Seeking Alpha: {e}")
        
        try:
            cef_articles = self.scrape_cefconnect()
            all_articles.extend(cef_articles)
            logger.info(f"Fetched {len(cef_articles)} CEFConnect articles")
        except Exception as e:
            logger.error(f"Error scraping CEFConnect: {e}")
        
        # 3. Enhanced NewsAPI queries
        try:
            api_articles = self.fetch_enhanced_newsapi()
            all_articles.extend(api_articles)
            logger.info(f"Fetched {len(api_articles)} NewsAPI articles")
        except Exception as e:
            logger.error(f"Error fetching NewsAPI articles: {e}")
        
        # 4. Deduplication
        unique_articles = self.advanced_deduplication(all_articles)
        logger.info(f"After deduplication: {len(unique_articles)} unique articles")
        
        # 5. Processing and classification
        processed_articles = self.process_articles(unique_articles)
        logger.info(f"Processed {len(processed_articles)} relevant articles")
        
        # 6. Sort by priority
        processed_articles.sort(key=lambda x: x.priority_score, reverse=True)
        
        return processed_articles

    def debug_fetch_all_sources(self):
        """Debug method to test all sources individually"""
        print("=== DEBUG: Testing all news sources ===")
        
        # Test RSS feeds
        print("\n1. Testing RSS feeds...")
        rss_articles = self.fetch_rss_feeds()
        print(f"RSS articles fetched: {len(rss_articles)}")
        
        # Test web scraping
        print("\n2. Testing web scraping...")
        sa_articles = self.scrape_seeking_alpha_cef()
        print(f"Seeking Alpha articles: {len(sa_articles)}")
        
        # Test NewsAPI
        print("\n3. Testing NewsAPI...")
        api_articles = self.fetch_enhanced_newsapi()
        print(f"NewsAPI articles: {len(api_articles)}")
        
        # Show sample articles
        all_articles = rss_articles + sa_articles + api_articles
        print(f"\nTotal articles before filtering: {len(all_articles)}")
        
        for i, article in enumerate(all_articles[:5]):
            print(f"\nArticle {i+1}:")
            print(f"Title: {article.get('title', 'N/A')}")
            print(f"Source: {article.get('source', 'N/A')}")
            print(f"URL: {article.get('url', 'N/A')}")

# For backwards compatibility
RobustCEFNewsFetcher = CEFNewsFetcher

if __name__ == "__main__":
    fetcher = CEFNewsFetcher()
    fetcher.debug_fetch_all_sources()
