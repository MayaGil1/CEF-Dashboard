import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

try:
    from core.discount_fetcher import CEFDiscountFetcher
except ImportError as e:
    st.error(f"Cannot import discount_fetcher: {str(e)}")
    st.info("Make sure discount_fetcher.py is in the core/ directory")

    def render():
        st.header("💰 CEF Discount Analysis")
        st.error("Discount fetcher not available. Please check your setup.")
        st.info("""
        To fix this issue:
        1. Create discount_fetcher.py in the core/ directory
        2. Install required dependencies: pip install requests pandas plotly beautifulsoup4
        3. Check network connectivity for CEFConnect access
        """)
else:
    # Initialize fetcher with error handling
    _fetcher = None

    def get_fetcher():
        global _fetcher
        if _fetcher is None:
            try:
                _fetcher = CEFDiscountFetcher()
                return _fetcher
            except Exception as e:
                st.error(f"Error initializing discount fetcher: {str(e)}")
                return None
        return _fetcher

    @st.cache_data(show_spinner=True, ttl=300)  # Cache for 5 minutes
    def _get_discount_data():
        """Fetch current discount data for all tracked funds."""
        fetcher = get_fetcher()
        if fetcher is None:
            return pd.DataFrame()
        
        try:
            discount_data = fetcher.fetch_all_discounts()
            if not discount_data:
                return pd.DataFrame()
            
            df = pd.DataFrame(discount_data)
            return df
            
        except Exception as e:
            st.error(f"Error fetching discount data: {str(e)}")
            return pd.DataFrame()

    @st.cache_data(show_spinner=True, ttl=300)
    def _get_historical_data(ticker: str, period: str = "1Y"):
        """Fetch historical price and NAV data for a specific fund."""
        fetcher = get_fetcher()
        if fetcher is None:
            return pd.DataFrame()
        
        try:
            historical_data = fetcher.fetch_historical_data(ticker, period)
            return pd.DataFrame(historical_data) if historical_data else pd.DataFrame()
        except Exception as e:
            st.error(f"Error fetching historical data for {ticker}: {str(e)}")
            return pd.DataFrame()

    def render():
        st.header("💰 CEF Discount Analysis")
        st.caption("Real-time discount/premium data from CEFConnect")
        
        # Fetch discount data
        col1, col2 = st.columns([1, 4])
        
        with col1:
            if st.button("🔄 Refresh Data"):
                _get_discount_data.clear()
                st.rerun()
        
        with col2:
            data_mode = st.selectbox(
                "Data Mode:",
                ["Live Data", "Sample Data"],
                index=0
            )
        
        # Get data based on mode
        if data_mode == "Live Data":
            with st.spinner("Fetching real-time data from CEFConnect..."):
                df = _get_discount_data()
        else:
            # Use sample data for demonstration
            df = create_sample_data()
        
        if df.empty:
            st.warning("No discount data available.")
            st.info("This could be due to:")
            st.write("- Network connectivity issues")
            st.write("- CEFConnect website unavailable")
            st.write("- Rate limiting restrictions")
            return
        
        # Display summary metrics
        display_summary_metrics(df)
        
        # Main discount chart
        create_discount_chart(df)
        
        # Detailed data table
        create_data_table(df)
        
        # Individual fund analysis
        create_fund_analysis_section(df)

    def display_summary_metrics(df):
        """Display key summary statistics."""
        st.subheader("📊 Market Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_discount = df['discount_percent'].mean()
            st.metric(
                "Average Discount",
                f"{avg_discount:.2f}%",
                delta=None
            )
        
        with col2:
            funds_at_discount = len(df[df['discount_percent'] < 0])
            st.metric(
                "Funds at Discount",
                f"{funds_at_discount}/{len(df)}",
                delta=None
            )
        
        with col3:
            best_discount = df['discount_percent'].min()
            best_fund = df.loc[df['discount_percent'].idxmin(), 'ticker']
            st.metric(
                "Best Discount",
                f"{best_discount:.2f}%",
                delta=f"{best_fund}"
            )
        
        with col4:
            highest_premium = df['discount_percent'].max()
            premium_fund = df.loc[df['discount_percent'].idxmax(), 'ticker']
            if highest_premium > 0:
                st.metric(
                    "Highest Premium",
                    f"+{highest_premium:.2f}%",
                    delta=f"{premium_fund}"
                )
            else:
                st.metric(
                    "Smallest Discount",
                    f"{highest_premium:.2f}%",
                    delta=f"{premium_fund}"
                )

    def create_discount_chart(df):
        """Create interactive discount/premium chart."""
        st.subheader("📈 Discount/Premium Analysis")
        
        # Sort by discount for better visualization
        df_sorted = df.sort_values('discount_percent')
        
        # Create color scale - red for discounts, green for premiums
        colors = ['red' if x < 0 else 'green' for x in df_sorted['discount_percent']]
        
        fig = go.Figure(data=[
            go.Bar(
                y=df_sorted['ticker'],
                x=df_sorted['discount_percent'],
                orientation='h',
                marker_color=colors,
                text=[f"{x:.1f}%" for x in df_sorted['discount_percent']],
                textposition='outside',
                hovertemplate=(
                    "<b>%{y}</b><br>" +
                    "Discount/Premium: %{x:.2f}%<br>" +
                    "Market Price: $%{customdata[0]:.2f}<br>" +
                    "NAV: $%{customdata[1]:.2f}<br>" +
                    "<extra></extra>"
                ),
                customdata=df_sorted[['market_price', 'nav']].values
            )
        ])
        
        fig.update_layout(
            title="Fund Discount/Premium Levels",
            xaxis_title="Discount/Premium (%)",
            yaxis_title="Fund Ticker",
            height=600,
            xaxis=dict(zeroline=True, zerolinewidth=2, zerolinecolor='black'),
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)

    def create_data_table(df):
        """Create detailed data table with CEFConnect links."""
        st.subheader("📋 Fund Details")
        
        # Create clickable links for fund tickers
        df_display = df.copy()
        df_display['Fund Link'] = df_display.apply(
            lambda row: f"https://www.cefconnect.com/fund/{row['ticker']}", 
            axis=1
        )
        
        # Select columns for display
        display_columns = [
            'ticker', 'fund_name', 'market_price', 'nav', 
            'discount_percent', 'distribution_rate'
        ]
        
        # Create column configuration
        column_config = {
            "ticker": st.column_config.LinkColumn(
                "Ticker",
                width="small",
                help="Click to view fund on CEFConnect"
            ),
            "fund_name": st.column_config.TextColumn("Fund Name", width="large"),
            "market_price": st.column_config.NumberColumn(
                "Market Price", 
                format="$%.2f",
                width="small"
            ),
            "nav": st.column_config.NumberColumn(
                "NAV", 
                format="$%.2f",
                width="small"
            ),
            "discount_percent": st.column_config.NumberColumn(
                "Discount/Premium", 
                format="%.2f%%",
                width="small"
            ),
            "distribution_rate": st.column_config.NumberColumn(
                "Distribution Rate", 
                format="%.2f%%",
                width="small"
            )
        }
        
        # Add fund links to display data
        for idx, row in df_display.iterrows():
            df_display.at[idx, 'ticker'] = row['Fund Link']
        
        st.dataframe(
            df_display[display_columns],
            column_config=column_config,
            use_container_width=True,
            height=400
        )

    def create_fund_analysis_section(df):
        """Create individual fund analysis section."""
        st.subheader("🔍 Individual Fund Analysis")
        
        selected_funds = st.multiselect(
            "Select funds for comparison:",
            options=df['ticker'].tolist(),
            default=df['ticker'].tolist()[:3]  # Default to first 3 funds
        )
        
        if selected_funds:
            # Filter data for selected funds
            selected_df = df[df['ticker'].isin(selected_funds)]
            
            # Create comparison chart
            fig = go.Figure()
            
            # Add NAV bars
            fig.add_trace(go.Bar(
                name='NAV',
                x=selected_df['ticker'],
                y=selected_df['nav'],
                marker_color='lightblue',
                opacity=0.7
            ))
            
            # Add Market Price bars
            fig.add_trace(go.Bar(
                name='Market Price',
                x=selected_df['ticker'],
                y=selected_df['market_price'],
                marker_color='darkblue'
            ))
            
            fig.update_layout(
                title="NAV vs Market Price Comparison",
                xaxis_title="Fund Ticker",
                yaxis_title="Price ($)",
                barmode='group',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Historical data option
            if len(selected_funds) == 1:
                st.subheader(f"📈 Historical Data - {selected_funds[0]}")
                
                period = st.selectbox(
                    "Select Period:",
                    ["1M", "3M", "6M", "1Y", "2Y"],
                    index=3
                )
                
                historical_df = _get_historical_data(selected_funds[0], period)
                
                if not historical_df.empty:
                    # Create historical price chart
                    fig_hist = go.Figure()
                    
                    fig_hist.add_trace(go.Scatter(
                        x=pd.to_datetime(historical_df['date']),
                        y=historical_df['market_price'],
                        name='Market Price',
                        line=dict(color='blue')
                    ))
                    
                    fig_hist.add_trace(go.Scatter(
                        x=pd.to_datetime(historical_df['date']),
                        y=historical_df['nav'],
                        name='NAV',
                        line=dict(color='green')
                    ))
                    
                    fig_hist.update_layout(
                        title=f"{selected_funds[0]} - Price History",
                        xaxis_title="Date",
                        yaxis_title="Price ($)",
                        height=400
                    )
                    
                    st.plotly_chart(fig_hist, use_container_width=True)

    def create_sample_data():
        """Create sample data for demonstration purposes."""
        import random
        from datetime import datetime
        
        fund_data = [
            ("PDO", "PIMCO Dynamic Income Opportunities Fund"),
            ("PDI", "PIMCO Dynamic Income Fund"),
            ("PHK", "PIMCO High Income Fund"),
            ("BST", "BlackRock Science & Tech Trust"),
            ("BDJ", "BlackRock Enhanced Equity Dividend Trust"),
            ("JFR", "Nuveen Floating Rate Income Fund"),
            ("ETG", "Eaton Vance Tax-Advantaged Global Dividend Opportunities"),
            ("ASA", "ASA Gold and Precious Metals Limited"),
            ("SWZ", "Swiss Helvetia Fund"),
            ("ECF", "Ellsworth Growth & Income Fund Ltd"),
            ("BCV", "Bancroft Fund Ltd"),
            ("NBXG", "Neuberger Berman NextGen Connectivity Fund Inc."),
            ("JOF", "Japan Smaller Capitalization Fund"),
            ("GAM", "General American Investors Company Inc."),
            ("BIGZ", "BlackRock Innovation & Growth Trust"),
            ("BMEZ", "BlackRock Health Sciences Trust II"),
            ("TTP", "Tortoise Pipeline & Energy Fund Inc.")
        ]
        
        sample_data = []
        for ticker, name in fund_data:
            nav = round(random.uniform(8, 25), 2)
            discount = round(random.uniform(-15, 5), 2)
            market_price = round(nav * (1 + discount/100), 2)
            
            sample_data.append({
                'ticker': ticker,
                'fund_name': name,
                'market_price': market_price,
                'nav': nav,
                'discount_percent': discount,
                'distribution_rate': round(random.uniform(4, 12), 2),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M')
            })
        
        return pd.DataFrame(sample_data)
