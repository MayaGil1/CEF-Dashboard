import sqlite3
from pathlib import Path
import logging

def setup_database():
    """Set up the SQLite database for SEC filings"""
    
    # Create data directory
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Connect to database
    db_path = data_dir / "sec_filings.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sec_filings (
            filing_id TEXT PRIMARY KEY,
            cik TEXT NOT NULL,
            fund_name TEXT,
            ticker TEXT,
            filing_type TEXT,
            filing_date TEXT,
            acceptance_date TEXT,
            accession_number TEXT,
            filer_name TEXT,
            ownership_percent REAL,
            shares_owned INTEGER,
            purpose TEXT,
            url TEXT,
            is_activist BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cef_mapping (
            ticker TEXT PRIMARY KEY,
            cik TEXT NOT NULL,
            fund_name TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_filing_date ON sec_filings(filing_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cik ON sec_filings(cik)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON sec_filings(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_activist ON sec_filings(is_activist)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_filing_type ON sec_filings(filing_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_filer_name ON sec_filings(filer_name)')
    
    conn.commit()
    conn.close()
    
    logging.info(f"Database initialized at {db_path}")

if __name__ == "__main__":
    setup_database()
