import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fake_useragent import UserAgent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CEFDiscountFetcher:
    """Fetch real-time CEF discount data from CEFConnect"""
    
    def __init__(self):
        self.base_url = "https://www.cefconnect.com"
        self.api_base = f"{self.base_url}/api/v3"
        self.session = requests.Session()
        self.setup_session()
        
        # Define the CEF universe to track
        self.cef_funds = {
            "PDO": ("PIMCO Dynamic Income Opportunities Fund"),
            "PDI": ("PIMCO Dynamic Income Fund"),
            "PHK": ("PIMCO High Income Fund"),
            "BST": ("BlackRock Science & Tech Trust"),
            "BDJ": ("BlackRock Enhanced Equity Dividend Trust"),
            "JFR": ("Nuveen Floating Rate Income Fund"),
            "ETG": ("Eaton Vance Tax-Advantaged Global Dividend Opportunities"),
            "ASA": ("ASA Gold and Precious Metals Limited"),
            "SWZ": ("Swiss Helvetia Fund"),
            "ECF": ("Ellsworth Growth & Income Fund Ltd"),
            "BCV": ("Bancroft Fund Ltd"),
            "NBXG": ("Neuberger Berman NextGen Connectivity Fund Inc."),
            "JOF": ("Japan Smaller Capitalization Fund"),
            "GAM": ("General American Investors Company Inc."),
            "BIGZ": ("BlackRock Innovation & Growth Trust"),
            "BMEZ": ("BlackRock Health Sciences Trust II"),
            "TTP": ("Tortoise Pipeline & Energy Fund Inc."),
        }
    
    def setup_session(self):
        """Setup session with realistic headers"""
        try:
            ua = UserAgent()
            user_agent = ua.chrome
        except:
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
    
    def fetch_all_discounts(self) -> List[Dict]:
        """Fetch current discount data for all tracked funds"""
        logger.info("Starting discount data fetch")
        
        try:
            # Try API endpoint first
            discount_data = self.fetch_via_api()
            if discount_data:
                logger.info(f"Successfully fetched {len(discount_data)} funds via API")
                return discount_data
        except Exception as e:
            logger.warning(f"API fetch failed: {e}")
        
        try:
            # Fallback to scraping
            discount_data = self.fetch_via_scraping()
            if discount_data:
                logger.info(f"Successfully fetched {len(discount_data)} funds via scraping")
                return discount_data
        except Exception as e:
            logger.error(f"Scraping fallback failed: {e}")
        
        logger.error("All fetch methods failed")
        return []
    
    def fetch_via_api(self) -> List[Dict]:
        """Fetch data using CEFConnect API"""
        # Build API query for our specific funds
        tickers = list(self.cef_funds.keys())
        ticker_query = ','.join(tickers)
        
        # API endpoint structure based on research
        api_url = f"{self.api_base}/DailyPricing"
        params = {
            'props': 'Ticker,Name,Price,NAV,Discount,DistributionRatePrice,LastUpdated',
            'tickers': ticker_query,
            '_': int(time.time() * 1000)  # Cache buster
        }
        
        response = self.session.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            return []
        
        processed_data = []
        for item in data:
            if item.get('Ticker') in self.cef_funds:
                processed_data.append({
                    'ticker': item.get('Ticker', ''),
                    'fund_name': item.get('Name', ''),
                    'market_price': float(item.get('Price', 0)),
                    'nav': float(item.get('NAV', 0)),
                    'discount_percent': float(item.get('Discount', 0)),
                    'distribution_rate': float(item.get('DistributionRatePrice', 0)),
                    'last_updated': item.get('LastUpdated', datetime.now().isoformat())
                })
        
        return processed_data
    
    def fetch_via_scraping(self) -> List[Dict]:
        """Fallback scraping method"""
        logger.info("Using scraping fallback method")
        
        # Use daily pricing page
        pricing_url = f"{self.base_url}/closed-end-funds-daily-pricing"
        
        response = self.session.get(pricing_url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for data table
        processed_data = []
        
        # Find the main data table
        table = soup.find('table') or soup.find('div', {'class': 'data-table'})
        
        if table:
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 6:
                    try:
                        ticker = cells[0].get_text(strip=True)
                        
                        if ticker in self.cef_funds:
                            name = cells[1].get_text(strip=True)
                            price_text = cells[2].get_text(strip=True).replace('$', '')
                            nav_text = cells[3].get_text(strip=True).replace('$', '')
                            discount_text = cells[4].get_text(strip=True).replace('%', '')
                            dist_text = cells[5].get_text(strip=True).replace('%', '') if len(cells) > 5 else '0'
                            
                            processed_data.append({
                                'ticker': ticker,
                                'fund_name': name,
                                'market_price': float(price_text),
                                'nav': float(nav_text),
                                'discount_percent': float(discount_text),
                                'distribution_rate': float(dist_text),
                                'last_updated': datetime.now().isoformat()
                            })
                    except (ValueError, IndexError):
                        continue
        
        return processed_data
    
    def fetch_historical_data(self, ticker: str, period: str = "1Y") -> List[Dict]:
        """Fetch historical price data for a specific fund"""
        if ticker not in self.cef_funds:
            logger.error(f"Ticker {ticker} not in tracked funds")
            return []
        
        try:
            # Historical data API endpoint
            hist_url = f"{self.api_base}/pricinghistory/{ticker}/{period}"
            
            response = self.session.get(hist_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or 'Data' not in data:
                return []
            
            price_history = data['Data'].get('PriceHistory', [])
            
            processed_history = []
            for item in price_history:
                processed_history.append({
                    'date': item.get('DataDateDisplay', ''),
                    'market_price': float(item.get('Price', 0)),
                    'nav': float(item.get('NAV', 0)),
                    'discount_percent': float(item.get('DiscountPremium', 0))
                })
            
            return processed_history
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {ticker}: {e}")
            return []
    
    def get_fund_url(self, ticker: str) -> str:
        """Get CEFConnect URL for a specific fund"""
        return f"{self.base_url}/fund/{ticker}"
    
    def test_connection(self) -> bool:
        """Test connection to CEFConnect"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            return response.status_code == 200
        except:
            return False
