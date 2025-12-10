#!/usr/bin/env python3
"""
Finnhub US Stock Service
Provides comprehensive US stock market data via Finnhub API.
"""
import os
import logging
import datetime
import asyncio
from typing import Any, Dict, Optional
import finnhub
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinnhubService:
    """
    Finnhub US Stock Service.
    
    Encapsulates the Finnhub API to retrieve categorized stock data:
    - Company Profile & Metadata
    - Market Data (Quotes)
    - Financials & Dividends
    - Analyst Estimates & Ratings
    - News & Sentiment Analysis
    """

    def __init__(self, api_key: Optional[str] = None):
        self.name = "Finnhub US Stock Service"
        # Get API key from environment if not provided
        self.api_key = api_key or os.getenv('FINNHUB_API_KEY')
        
        if not self.api_key:
            logger.warning("FINNHUB_API_KEY not found in environment variables")
        
        try:
            self.client = finnhub.Client(api_key=self.api_key)
            logger.info("FinnhubService initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Finnhub client: {str(e)}")
            self.client = None

    def _date_to_timestamp(self, date_str: str, end_of_day: bool = False) -> int:
        """
        Convert 'YYYY-MM-DD' string to Unix timestamp.
        Args:
            date_str: Date string in YYYY-MM-DD format.
            end_of_day: If True, set time to 23:59:59.
        """
        try:
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            if end_of_day:
                dt = dt.replace(hour=23, minute=59, second=59)
            return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        except ValueError:
            logger.error(f"Invalid date format: {date_str}, expected YYYY-MM-DD")
            return 0

    def _safe_api_call(self, func, *args, **kwargs) -> Any:
        """Helper to wrap API calls with try-except block."""
        if not self.client:
            return None
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"API call failed: {str(e)}")
            return None

    def _format_response(self, data: Any, meta: Dict[str, Any] = None) -> Dict[str, Any]:
        """Standardized response format."""
        if data is None:
            return {
                "success": False,
                "error": "No data returned or API error",
                "metadata": {
                    "query_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    **(meta or {})
                }
            }
        return {
            "success": True,
            "found": True if data else False,
            "data": data,
            "metadata": {
                "query_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                **(meta or {})
            }
        }

    async def _get_company_profile(self, symbol: str) -> Dict[str, Any]:
        """
        Get general company information.
        Includes: Profile, Peers.

        Args:
            symbol: Stock symbol (e.g., 'AAPL').
        Returns:
            Dict: Contains 'profile' and 'peers' data.
        """
        data = {}
        
        # 1. Company Profile 2
        data['profile'] = self._safe_api_call(self.client.company_profile2, symbol=symbol)
        
        # 2. Company Peers
        data['peers'] = self._safe_api_call(self.client.company_peers, symbol)
        
        return self._format_response(data, {"symbol": symbol})

    async def _get_market_data(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get market data including real-time quote.

        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
        Returns:
            Dict: Contains 'quote' (real-time).
        """
        data = {}
        # 1. Real-time Quote
        data['quote'] = self._safe_api_call(self.client.quote, symbol)

        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})

    async def _get_financials(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get financial data including basic metrics, earnings surprises, and dividends.

        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
        Returns:
            Dict: Contains 'basic_financials', 'earnings_surprises', and 'dividends'.
        """
        data = {}

        # 1. Basic Financials (Metric 'all')
        data['basic_financials'] = self._safe_api_call(self.client.company_basic_financials, symbol, 'all')

        # 2. Earnings Surprises (Limit to last 4 quarters)
        data['earnings_surprises'] = self._safe_api_call(self.client.company_earnings, symbol, limit=4)

        # 3. Dividends
        data['dividends'] = self._safe_api_call(self.client.stock_dividends, symbol, _from=start_date, to=end_date)

        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})

    async def _get_estimates_and_analysis(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get analyst estimates and trends.
        Includes: Recommendation Trends.

        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
        Returns:
            Dict: Contains 'recommendation_trends'.
        """
        data = {}

        # 1. Recommendation Trends
        data['recommendation_trends'] = self._safe_api_call(self.client.recommendation_trends, symbol)

        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})

    async def _get_sentiment_and_news(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get news and sentiment analysis.
        Includes: Company News.

        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.

        Returns:
            Dict: Contains 'news' (last 10 items).
        """
        data = {}

        # 1. Company News (Last 10 items)
        news = self._safe_api_call(self.client.company_news, symbol, _from=start_date, to=end_date)
        if isinstance(news, list):
            data['news'] = news[:10]
        else:
            data['news'] = news
            
        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})

    async def _get_all_stock_info(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Aggregates ALL available stock information into a single report.
        Useful for comprehensive analysis.

        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
        
        Returns:
            Dict: A comprehensive dictionary containing: 
                  - company_profile
                  - market_data
                  - financials
                  - analysis
                  - sentiment
        """
        # Collect raw data from sub-methods
        # We unwrap the 'data' field from each sub-method response if successful
        
        raw_data = {}
        
        profile_res = await self._get_company_profile(symbol)
        if profile_res.get("success"):
            raw_data['company_profile'] = profile_res.get("data")

        market_res = await self._get_market_data(symbol, start_date, end_date)
        if market_res.get("success"):
            raw_data['market_data'] = market_res.get("data")
            
        financial_res = await self._get_financials(symbol, start_date, end_date)
        if financial_res.get("success"):
            raw_data['financials'] = financial_res.get("data")
            
        analysis_res = await self._get_estimates_and_analysis(symbol, start_date, end_date)
        if analysis_res.get("success"):
            raw_data['analysis'] = analysis_res.get("data")
            
        sentiment_res = await self._get_sentiment_and_news(symbol, start_date, end_date)
        if sentiment_res.get("success"):
            raw_data['sentiment'] = sentiment_res.get("data")

        return self._format_response(raw_data, {
            "symbol": symbol, 
            "period": f"{start_date} to {end_date}",
            "generated_at": datetime.datetime.now().isoformat()
        })

    async def _fetch_transcripts(self, transcript_id: str) -> Dict[str, Any]:
        """
        Fetch earnings call transcript content.
        
        Args:
            transcript_id: Transcript ID (e.g., 'AAPL_162777').
            
        Returns:
            Dict: Transcript content and metadata.
        """
        data = self._safe_api_call(self.client.transcripts, transcript_id)
        return self._format_response(data, {"transcript_id": transcript_id})
    
    async def _fetch_transcripts_list(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch list of available earnings call transcripts for a symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            
        Returns:
            Dict: List of available transcripts with IDs and dates.
        """
        data = self._safe_api_call(self.client.transcripts_list, symbol)
        return self._format_response(data, {"symbol": symbol})
    
    async def _fetch_company_executives(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch company executive information.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            
        Returns:
            Dict: Company executive profiles including name, position, compensation.
        """
        data = self._safe_api_call(self.client.company_executive, symbol)
        return self._format_response(data, {"symbol": symbol})
    
    async def _fetch_sec_filings(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Fetch SEC filings (10-K, 10-Q, 8-K, etc).
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            
        Returns:
            Dict: SEC filing records.
        """
        data = self._safe_api_call(self.client.filings, symbol=symbol, _from=start_date, to=end_date)
        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})
    
    async def _fetch_insider_transactions(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Fetch insider trading transactions.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            
        Returns:
            Dict: Insider transaction records (buy/sell by executives).
        """
        data = self._safe_api_call(self.client.stock_insider_transactions, symbol, start_date, end_date)
        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})
    
    async def _fetch_institutional_ownership(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Fetch institutional ownership data (13-F filings).
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            
        Returns:
            Dict: Institutional holdings and ownership changes.
        """
        data = self._safe_api_call(self.client.ownership, symbol, limit=10)
        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})
    
    async def _fetch_fund_ownership(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch mutual fund ownership data.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            
        Returns:
            Dict: Mutual fund holdings.
        """
        data = self._safe_api_call(self.client.fund_ownership, symbol, limit=10)
        return self._format_response(data, {"symbol": symbol})
    
    async def _fetch_news_sentiment(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch news sentiment analysis scores.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            
        Returns:
            Dict: News sentiment metrics (bullish/bearish scores, article count).
        """
        data = self._safe_api_call(self.client.news_sentiment, symbol)
        return self._format_response(data, {"symbol": symbol})
    
    async def _fetch_social_sentiment(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch social media sentiment (Reddit, Twitter).
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            
        Returns:
            Dict: Social media sentiment scores and mention counts.
        """
        data = self._safe_api_call(self.client.stock_social_sentiment, symbol)
        return self._format_response(data, {"symbol": symbol})
    
    async def _fetch_insider_sentiment(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Fetch aggregated insider trading sentiment.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            
        Returns:
            Dict: Insider sentiment metrics (MSPR - Monthly Share Purchase Ratio).
        """
        data = self._safe_api_call(self.client.stock_insider_sentiment, symbol, start_date, end_date)
        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})
    
    async def _fetch_upgrade_downgrade(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Fetch analyst rating changes (upgrades/downgrades).
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            
        Returns:
            Dict: Analyst rating change events.
        """
        data = self._safe_api_call(self.client.upgrade_downgrade, symbol=symbol, _from=start_date, to=end_date)
        return self._format_response(data, {"symbol": symbol, "period": f"{start_date} to {end_date}"})
    
    async def _fetch_pattern_recognition(self, symbol: str, resolution: str = 'D') -> Dict[str, Any]:
        """
        Fetch technical pattern recognition results.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            resolution: Time resolution ('D' for daily, 'W' for weekly).
            
        Returns:
            Dict: Detected chart patterns (head and shoulders, triangles, etc).
        """
        data = self._safe_api_call(self.client.pattern_recognition, symbol, resolution)
        return self._format_response(data, {"symbol": symbol, "resolution": resolution})
    
    async def _fetch_support_resistance(self, symbol: str, resolution: str = 'D') -> Dict[str, Any]:
        """
        Fetch support and resistance levels.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            resolution: Time resolution ('D' for daily, 'W' for weekly).
            
        Returns:
            Dict: Support and resistance price levels.
        """
        data = self._safe_api_call(self.client.support_resistance, symbol, resolution)
        return self._format_response(data, {"symbol": symbol, "resolution": resolution})
    
    async def _fetch_aggregate_indicator(self, symbol: str, resolution: str = 'D') -> Dict[str, Any]:
        """
        Fetch aggregate technical indicator signals.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL').
            resolution: Time resolution ('D' for daily).
            
        Returns:
            Dict: Aggregate buy/sell/neutral signals from multiple indicators.
        """
        data = self._safe_api_call(self.client.aggregate_indicator, symbol, resolution)
        return self._format_response(data, {"symbol": symbol, "resolution": resolution})

if __name__ == "__main__":
    # Test Block
    async def test():
        service = FinnhubService()
        symbol = "AAPL"
        
        # Dynamic date range: Last 30 days
        end_dt = datetime.datetime.now()
        start_dt = end_dt - datetime.timedelta(days=30)
        
        start = start_dt.strftime("%Y-%m-%d")
        end = end_dt.strftime("%Y-%m-%d")
        
        print(f"--- Testing _get_all_stock_info for {symbol} ---")
        res = await service._get_all_stock_info(symbol, start, end)
        
        # Print simplified structure check
        print(f"Success: {res.get('success')}")
        if res.get('success'):
            data = res.get('data', {})
            print(f"Keys: {list(data.keys())}")
            if 'company_profile' in data:
                print(f"Profile Name: {data['company_profile'].get('profile', {}).get('name')}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(test())
