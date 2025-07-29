import pandas as pd
import streamlit as st
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

try:
    from core.sec_filings_fetcher import CEFSecFilingsFetcher
except ImportError as e:
    st.error(f"Cannot import sec_filings_fetcher: {str(e)}")
    st.info("Make sure sec_filings_fetcher.py is in the core/ directory")

    def render():
        st.header("📋 SEC Filings Monitor")
        st.error("SEC filings fetcher not available. Please check your setup.")
        st.info("""
        To fix this issue:
        1. Ensure sec_filings_fetcher.py is in the core/ directory
        2. Install required dependencies: pip install requests pandas plotly beautifulsoup4 lxml
        3. Check database permissions for data/ directory
        """)
else:
    # Initialize fetcher with error handling - using DEFAULT_TICKER_MAP
    _fetcher = None

    def get_fetcher():
        global _fetcher
        if _fetcher is None:
            try:
                # Create fetcher without custom ticker_map to use DEFAULT_TICKER_MAP
                _fetcher = CEFSecFilingsFetcher()
                
                # Debug: Verify ticker map
                print(f"[DEBUG] Fetcher initialized with {len(_fetcher.ticker_map)} tickers:")
                print(f"[DEBUG] CEF tickers: {list(_fetcher.ticker_map.keys())}")
                
                # Check for problematic tickers
                problematic_tickers = {'JPM', 'NVDA', 'BRK-B', 'BRKB'}
                found_problematic = problematic_tickers.intersection(set(_fetcher.ticker_map.keys()))
                if found_problematic:
                    print(f"[WARNING] Found non-CEF tickers: {found_problematic}")
                
                return _fetcher
            except Exception as e:
                st.error(f"Error initializing SEC filings fetcher: {str(e)}")
                return None
        return _fetcher

    
    @st.cache_data(show_spinner=True, ttl=3600)  # Cache for 1 hour
    def _get_filings(days_back: int, use_cache: bool = True):
        """Fetch SEC filings with caching"""
        fetcher = get_fetcher()
        if fetcher is None:
            return []
        
        try:
            print(f"[DEBUG] _get_filings called with days_back={days_back}, use_cache={use_cache}")
            
            if use_cache:
                filings = fetcher.get_cached_filings(days_back=days_back)
                print(f"[DEBUG] Used cache: {len(filings)} filings loaded")
                if filings:
                    return filings
                st.info("No cached filings found. Fetching from SEC API...")
            
            print("[DEBUG] Calling live fetch")
            filings = fetcher.fetch_cef_filings(days_back=days_back)
            print(f"[DEBUG] Live fetch completed: {len(filings)} filings loaded")
            
            # Debug: Show which tickers have filings
            if filings:
                ticker_counts = {}
                for filing in filings:
                    ticker = filing.ticker
                    ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
                print(f"[DEBUG] Filings by ticker: {ticker_counts}")
            
            return filings
        except Exception as e:
            st.error(f"Error fetching filings: {str(e)}")
            print(f"[DEBUG] Error in _get_filings: {str(e)}")
            return []

    def render():
        st.header("📋 SEC Filings Monitor")
        st.caption("Track Schedule 13D/13G/13A filings for CEF activist investor activity")

        # Display ticker mapping info
        fetcher = get_fetcher()
        if fetcher:
            with st.expander("📊 CEF Universe"):
                st.write(f"Monitoring {len(fetcher.ticker_map)} Closed-End Funds:")
                ticker_df = pd.DataFrame([
                    {"Ticker": ticker, "CIK": cik, "Fund Name": fund_name[:50] + "..." if len(fund_name) > 50 else fund_name}
                    for ticker, (cik, fund_name) in fetcher.ticker_map.items()
                ])
                st.dataframe(ticker_df, use_container_width=True, height=200)

        # Sidebar controls
        with st.sidebar:
            st.subheader("⚙️ Fetch Settings")
            
            days_back = st.slider(
                "Days to look back:",
                min_value=7,
                max_value=365,
                value=90,  # Increased default for CEFs
                step=7
            )

            use_cache = st.checkbox("Use cached data", value=True)
            
            # Debug controls
            st.subheader("🔧 Debug Controls")
            if st.button("🗑️ Clear Database"):
                try:
                    # Close the database connection first
                    fetcher = get_fetcher()
                    if fetcher and hasattr(fetcher, 'conn'):
                        fetcher.conn.close()
                    
                    # Clear the global fetcher instance
                    global _fetcher
                    _fetcher = None
                    
                    # Now safely remove the database file
                    import os
                    db_path = "data/sec_filings.db"
                    if os.path.exists(db_path):
                        os.remove(db_path)
                        st.success("Database cleared! Will fetch fresh CEF data.")
                        _get_filings.clear()
                        st.rerun()
                except Exception as e:
                    st.error(f"Error clearing database: {str(e)}")
                    st.info("Try restarting the Streamlit app to fully clear the database.")

            if st.button("🔄 Clear Contents Only"):
                try:
                    # Clear Streamlit cache
                    _get_filings.clear()
                    
                    # Clear database contents without deleting file
                    fetcher = get_fetcher()
                    if fetcher and hasattr(fetcher, 'conn'):
                        cursor = fetcher.conn.cursor()
                        cursor.execute("DELETE FROM sec_filings")
                        cursor.execute("VACUUM")  # Reclaim disk space
                        fetcher.conn.commit()
                        
                    st.success("Database contents cleared! Will fetch fresh CEF data.")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error clearing database contents: {str(e)}")

            if st.button("Clear Cache & Force Fetch"):
                _get_filings.clear()
                use_cache = False
                st.success("Cache cleared! Will fetch fresh data.")
                st.rerun()
            
            refresh_col1, refresh_col2 = st.columns(2)
            with refresh_col1:
                if st.button("🔄 Refresh"):
                    _get_filings.clear()
                    st.rerun()
            
            with refresh_col2:
                if st.button("💾 Force Fetch"):
                    _get_filings.clear()
                    st.rerun()

        # Fetch filings
        with st.spinner(f"Loading SEC filings for the last {days_back} days..."):
            filings = _get_filings(days_back=days_back, use_cache=use_cache)

        if not filings:
            st.info("No SEC filings found for the specified period.")
            st.write("This could be due to:")
            st.write("- No recent 13D/13G/13A filings for tracked CEFs")
            st.write("- SEC API connectivity issues")
            st.write("- Rate limiting or access restrictions")
            st.write("- Try increasing the lookback period to 180+ days")
            
            # Show which CEFs we're monitoring
            if fetcher:
                st.write(f"**Monitoring {len(fetcher.ticker_map)} CEFs:** {', '.join(fetcher.ticker_map.keys())}")
            return

        # Convert to DataFrame
        df = pd.DataFrame([filing.to_dict() for filing in filings])
        if filings:
            df = pd.DataFrame([filing.to_dict() for filing in filings])
            
            # Expected CEF tickers from DEFAULT_TICKER_MAP
            expected_cef_tickers = {
                'PDO', 'PDI', 'PHK', 'BST', 'BDJ', 'JFR', 'ETG', 'ASA', 
                'SWZ', 'ECF', 'BCV', 'NBXG', 'JOF', 'GAM', 'BIGZ', 'BMEZ', 'TTP'
            }
            
            df_tickers = set(df['ticker'].unique()) if 'ticker' in df.columns else set()
            non_cef_tickers = df_tickers - expected_cef_tickers
            

        # Verify we have CEF data, not JPM/non-CEF data
        cef_tickers = set(fetcher.ticker_map.keys()) if fetcher else set()
        df_tickers = set(df['ticker'].unique()) if 'ticker' in df.columns else set()
        non_cef_tickers = df_tickers - cef_tickers
        


        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Filings", len(df))
        
        with col2:
            activist_count = df['is_activist'].sum() if 'is_activist' in df.columns else 0
            st.metric("Activist Filings", activist_count)
        
        with col3:
            unique_funds = df['fund_name'].nunique() if 'fund_name' in df.columns else 0
            st.metric("Funds Involved", unique_funds)
        

        # Filters
        st.subheader("🔍 Filter Filings")
        
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            selected_types = st.multiselect(
                "Filing Types:",
                options=sorted(df['filing_type'].unique()) if 'filing_type' in df.columns else [],
                default=[]
            )
        
        with filter_col2:
            selected_filers = st.multiselect(
                "Filers:",
                options=sorted(df['filer_name'].unique()) if 'filer_name' in df.columns else [],
                default=[]
            )
        
        with filter_col3:
            activist_filter = st.selectbox(
                "Activist Filter:",
                options=["All", "Activist Only", "Non-Activist Only"],
                index=0
            )

        # Apply filters
        filtered_df = df.copy()
        
        if selected_types and 'filing_type' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['filing_type'].isin(selected_types)]
        
        if selected_filers and 'filer_name' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['filer_name'].isin(selected_filers)]
        
        if 'is_activist' in filtered_df.columns:
            if activist_filter == "Activist Only":
                filtered_df = filtered_df[filtered_df['is_activist'] == True]
            elif activist_filter == "Non-Activist Only":
                filtered_df = filtered_df[filtered_df['is_activist'] == False]

        if filtered_df.empty:
            st.warning("No filings match the selected filters.")
            return

        # Define visible columns - only include columns that exist
        ALL_VISIBLE_COLS = [
            "filing_date", "ticker", "fund_name", "filing_type", 
            "filer_name", "url"
        ]
        
        VISIBLE_COLS = [col for col in ALL_VISIBLE_COLS if col in filtered_df.columns]
        
        # Filter DataFrame to show only desired columns
        display_df = filtered_df[VISIBLE_COLS].copy()
        
        # Sort by filing date (most recent first)
        if 'filing_date' in display_df.columns:
            display_df = display_df.sort_values('filing_date', ascending=False)

        # Display filings table
        st.subheader(f"📋 SEC Filings ({len(display_df)} records)")
        
        # Create column config based on available columns
        column_config = {}
        if 'filing_date' in VISIBLE_COLS:
            column_config["filing_date"] = st.column_config.DateColumn("Filing Date", width="medium")
        if 'ticker' in VISIBLE_COLS:
            column_config["ticker"] = st.column_config.TextColumn("Ticker", width="small")
        if 'fund_name' in VISIBLE_COLS:
            column_config["fund_name"] = st.column_config.TextColumn("Fund Name", width="large")
        if 'filing_type' in VISIBLE_COLS:
            column_config["filing_type"] = st.column_config.TextColumn("Type", width="small")
        if 'filer_name' in VISIBLE_COLS:
            column_config["filer_name"] = st.column_config.TextColumn("Filer", width="large")
        if 'url' in VISIBLE_COLS:
            column_config["url"] = st.column_config.LinkColumn("SEC Filing", width="small")
        
        st.dataframe(
            display_df.reset_index(drop=True),
            height=600,
            use_container_width=True,
            column_config=column_config
        )

        # Activist investor spotlight
        if 'is_activist' in filtered_df.columns and 'filer_name' in filtered_df.columns:
            activist_filings = filtered_df[filtered_df['is_activist'] == True]
            if not activist_filings.empty:
                st.subheader("🎯 Activist Investor Activity")
                
                agg_dict = {'filing_id': 'count'}
                if 'fund_name' in activist_filings.columns:
                    agg_dict['fund_name'] = 'nunique'
                
                activist_summary = activist_filings.groupby('filer_name').agg(agg_dict).round(2)
                
                # Rename columns based on what we have
                column_names = ['Filings']
                if 'fund_name' in agg_dict:
                    column_names.append('Funds Targeted')
                
                activist_summary.columns = column_names
                activist_summary = activist_summary.sort_values('Filings', ascending=False)
                
                st.dataframe(
                    activist_summary,
                    use_container_width=True,
                    column_config={
                        "Filings": st.column_config.NumberColumn("Number of Filings"),
                        "Funds Targeted": st.column_config.NumberColumn("Unique Funds Targeted")
                    }
                )

        # Download option
        if st.button("📥 Download Data as CSV"):
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="Download SEC Filings CSV",
                data=csv,
                file_name=f"sec_filings_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

        # Debug information at the bottom
        with st.expander("🔧 Debug Information"):
            st.write("**DataFrame Info:**")
            st.write(f"Total rows: {len(df)}")
            st.write(f"Columns: {list(df.columns)}")
            if len(df) > 0:
                st.write("**Sample data:**")
                st.dataframe(df.head(3))
            
            if fetcher:
                st.write("**Fetcher Info:**")
                st.write(f"Ticker map contains: {list(fetcher.ticker_map.keys())}")
                st.write(f"Database path: {fetcher.conn}")