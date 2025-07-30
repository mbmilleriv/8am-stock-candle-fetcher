import requests
import pandas as pd
from datetime import datetime
import os
import pytz
import time
from typing import List, Dict, Optional

class FMPCandleFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
    def fetch_intraday_candles(self, symbol: str, interval: str = "30min") -> Optional[pd.DataFrame]:
        """Fetch intraday candle data for a single stock"""
        url = f"{self.base_url}/historical-chart/{interval}/{symbol}"
        params = {"apikey": self.api_key}
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                return df
            else:
                print(f"No data available for {symbol}")
                return None
                
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None
    
    def get_latest_830_candle(self, symbol: str) -> Optional[Dict]:
        """Get the most recent 8:00-8:30 AM ET candle"""
        df = self.fetch_intraday_candles(symbol, "30min")
        
        if df is None or df.empty:
            return None
        
        # Convert to Eastern Time
        eastern = pytz.timezone('US/Eastern')
        df['date_et'] = df['date'].dt.tz_localize('UTC').dt.tz_convert(eastern)
        
        # Get today's date in ET
        today_et = datetime.now(eastern).date()
        df_today = df[df['date_et'].dt.date == today_et]
        
        # Look for the 8:30 AM ET candle
        for idx, row in df_today.iterrows():
            candle_time = row['date_et'].time()
            if candle_time.hour == 8 and candle_time.minute == 30:
                return {
                    'symbol': symbol,
                    'date': row['date'].strftime('%Y-%m-%d %H:%M:%S'),
                    'date_et': row['date_et'].strftime('%Y-%m-%d %H:%M:%S %Z'),
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                }
        
        print(f"8:30 AM ET candle not found for {symbol} on {today_et}")
        return None
    
    def fetch_multiple_stocks(self, symbols: List[str]) -> pd.DataFrame:
        """Fetch the 8:00-8:30 AM candle for multiple stocks"""
        results = []
        
        for i, symbol in enumerate(symbols):
            print(f"Fetching data for {symbol} ({i+1}/{len(symbols)})...")
            candle_data = self.get_latest_830_candle(symbol)
            
            if candle_data:
                results.append(candle_data)
            
            # Respect API rate limits
            time.sleep(0.5)
        
        if results:
            return pd.DataFrame(results)
        else:
            return pd.DataFrame()

def load_stock_list(filename='watchlist.txt'):
    """Load stock symbols from a text file"""
    try:
        with open(filename, 'r') as f:
            # Read lines, strip whitespace, ignore empty lines and comments
            stocks = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Allow for comments after symbol with #
                    symbol = line.split('#')[0].strip().upper()
                    if symbol:
                        stocks.append(symbol)
            return stocks
    except FileNotFoundError:
        print(f"Warning: {filename} not found. Using default stock list.")
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def main():
    # Get API key from environment variable
    API_KEY = os.environ.get('FMP_API_KEY')
    if not API_KEY:
        raise ValueError("FMP_API_KEY environment variable not set")
    
    # Load stock list from file
    STOCK_LIST = load_stock_list('watchlist.txt')
    
    print(f"\n{'='*60}")
    print(f"Fetching 8:00-8:30 AM ET candles at {datetime.now()}")
    print(f"{'='*60}\n")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Fetch candles
    fetcher = FMPCandleFetcher(API_KEY)
    df = fetcher.fetch_multiple_stocks(STOCK_LIST)
    
    if not df.empty:
        # Save to CSV
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"data/candles_830am_{date_str}.csv"
        df.to_csv(filename, index=False)
        
        print(f"\nSuccessfully fetched {len(df)} candles")
        print(f"Data saved to {filename}")
        print("\nPreview:")
        print(df[['symbol', 'close', 'volume', 'date_et']].to_string())
    else:
        print("No candle data retrieved")
        exit(1)

if __name__ == "__main__":
    main()
