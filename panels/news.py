import pandas as pd
import streamlit as st
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

try:
    from core.news_fetcher import CEFNewsFetcher
except ImportError as e:
    st.error(f"Cannot import news_fetcher: {str(e)}")
    st.info("Make sure news_fetcher.py is in the core/ directory")

    def render():
        st.header("📰 CEF News & Announcements")
        st.error("News fetcher not available. Please check your setup.")
        st.info("""
        To fix this issue:
        1. Create a 'core' directory in your project root
        2. Move news_fetcher.py to core/news_fetcher.py
        3. Create an empty core/__init__.py file
        4. Install required dependencies: pip install transformers spacy
        5. Download spacy model: python -m spacy download en_core_web_sm
        """)
else:
    # Initialize fetcher with error handling
    _fetcher = None
    def init_session_state():
        """Initialize session state for hidden rows"""
        if "hidden_news_rows" not in st.session_state:
            st.session_state.hidden_news_rows = set()

    def hide_news_row(row_index):
        """Function to hide a news row"""
        st.session_state.hidden_news_rows.add(row_index)

    def show_hidden_rows_count():
        """Display count of hidden rows"""
        if st.session_state.hidden_news_rows:
            st.info(f"🙈 Hidden items: {len(st.session_state.hidden_news_rows)}")

    def reset_hidden_rows():
        """Reset all hidden rows"""
        st.session_state.hidden_news_rows.clear()

    def get_fetcher():
        global _fetcher
        if _fetcher is None:
            try:
                _fetcher = CEFNewsFetcher()
                return _fetcher
            except Exception as e:
                st.error(f"Error initializing news fetcher: {str(e)}")
                return None
        return _fetcher

    @st.cache_data(show_spinner=True, ttl=600)
    def _get_articles():
        """Run full back-end fetch and return DataFrame."""
        fetcher = get_fetcher()
        if fetcher is None:
            return pd.DataFrame()
        try:
            articles = fetcher.fetch_all_news()
            if not articles:
                return pd.DataFrame()
            df = pd.DataFrame([a.to_dict() for a in articles])
            return df
        except Exception as e:
            st.error(f"Error fetching articles: {str(e)}")
            return pd.DataFrame()

    def render():
        init_session_state()  
        st.header("📰 CEF News & Announcements")
        st.caption("Source blend: NewsAPI, Marketaux, Alpha Vantage, finance RSS")

        # Check if API keys are available
        api_keys = {
            'NEWSAPI_KEY': os.getenv('NEWSAPI_KEY'),
            'MARKETAUX_API_KEY': os.getenv('MARKETAUX_API_KEY'),
            'ALPHAVANTAGE_API_KEY': os.getenv('ALPHAVANTAGE_API_KEY')
        }


        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("🔄 Refresh now"):
                _get_articles.clear()
                st.rerun()

        with col2:
            st.info("Click refresh to fetch latest news (may take 30-60 seconds)")

        # Try to get articles
        with st.spinner("Loading news articles..."):
            df = _get_articles()

        if df.empty:
            st.info("No relevant news found. This could be due to:")
            st.write("- Missing API keys")
            st.write("- Network connectivity issues")
            st.write("- All articles filtered out as irrelevant")
            st.write("- API rate limits reached")
            return

        # Show basic stats
        st.success(f"Found {len(df)} relevant articles")

        # --- sidebar filters --------------------------------------------------
        with st.sidebar:
            st.subheader("🔍 Filters")

            # Handle tickers filter safely
            if 'tickers' in df.columns:
                try:
                    all_tickers = set()
                    for ticker_list in df.tickers:
                        if isinstance(ticker_list, list):
                            all_tickers.update(ticker_list)
                    tickers = st.multiselect(
                        "Filter by ticker:",
                        sorted(all_tickers) if all_tickers else []
                    )
                except Exception:
                    tickers = []
            else:
                tickers = []

            # Handle categories filter safely
            if 'category' in df.columns:
                categories = st.multiselect(
                    "Filter by category:",
                    sorted(df.category.unique())
                )
            else:
                categories = []

            # Priority filter
            if 'priority_score' in df.columns:
                min_priority = st.slider("Priority ≥", 0.0, 10.0, 0.0, 0.1)
            else:
                min_priority = 0.0

        # Apply filters
        mask = pd.Series([True] * len(df))

        if 'priority_score' in df.columns:
            mask &= (df.priority_score >= min_priority)

        if tickers and 'tickers' in df.columns:
            mask &= df.tickers.apply(
                lambda lst: any(t in lst for t in tickers) if isinstance(lst, list) else False
            )

        if categories and 'category' in df.columns:
            mask &= df.category.isin(categories)

        filtered_df = df[mask]

        if filtered_df.empty:
            st.info("No articles match your filter criteria")
            return

        # Filter out hidden rows
        visible_df = filtered_df[~filtered_df.index.isin(st.session_state.hidden_news_rows)]

        # Control buttons for hidden functionality
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            show_hidden_rows_count()
        with col2:
            if st.button("🔄 Reset Hidden", help="Show all hidden rows again"):
                reset_hidden_rows()
                st.rerun()
        with col3:
            st.metric("Visible Articles", len(visible_df))

        st.markdown("---")

        # Check if all articles are hidden
        if len(visible_df) == 0:
            st.warning("All articles are hidden. Click '🔄 Reset Hidden' to show them again.")
            return

        # Sort by priority if available
        if 'priority_score' in visible_df.columns:
            visible_df = visible_df.sort_values("priority_score", ascending=False)


        VISIBLE_COLS = [
            "title", "category", "published_at", "tickers", "url", "source","fund_names", "activists"
        ]
        existing_cols = [col for col in VISIBLE_COLS if col in filtered_df.columns]
        display_df = filtered_df[existing_cols].copy()
        # Display articles in table format with integrated hide buttons
        st.subheader(f"📰 News Articles ({len(visible_df)} visible)")

        # Create table headers
        header_cols = st.columns([5, 1.2, 1.5, 1.8, 1.2, 0.8])
        with header_cols[0]:
            st.markdown("**📰 Title**")
        with header_cols[1]:
            st.markdown("**📁 Category**")
        with header_cols[2]:
            st.markdown("**📅 Published**")
        with header_cols[3]:
            st.markdown("**🏷️ Tickers**")
        with header_cols[4]:
            st.markdown("**🔗 Source**")
        with header_cols[5]:
            st.markdown("**Action**")

        st.markdown("---")

        # Display each article as a table row
        for idx, row in visible_df.iterrows():
            cols = st.columns([5, 1.2, 1.5, 1.8, 1.2, 0.8])
            
            with cols[0]:
                title = row.get('title', 'No Title')
                if 'url' in row and pd.notna(row['url']):
                    st.markdown(f"[{title}]({row['url']})")
                else:
                    st.write(title)
                
                # Show additional info in smaller text below title
                additional_info = []
                if 'fund_names' in row and row['fund_names']:
                    fund_names = ', '.join(row['fund_names']) if isinstance(row['fund_names'], list) else str(row['fund_names'])
                    additional_info.append(f"🏢 {fund_names}")
                if 'activist_mentions' in row and row['activist_mentions']:
                    activists = ', '.join(row['activist_mentions']) if isinstance(row['activist_mentions'], list) else str(row['activist_mentions'])
                    additional_info.append(f"⚡ {activists}")
                
                if additional_info:
                    st.caption(" • ".join(additional_info))
            
            with cols[1]:
                st.write(row.get('category', 'N/A'))
            
            with cols[2]:
                pub_date = str(row.get('published_at', 'N/A'))
                # Format date nicely
                if pub_date != 'N/A' and len(pub_date) > 10:
                    st.write(pub_date[:10])
                else:
                    st.write(pub_date)
            
            with cols[3]:
                if 'tickers' in row and row['tickers']:
                    tickers_display = ', '.join(row['tickers']) if isinstance(row['tickers'], list) else str(row['tickers'])
                    st.write(tickers_display)
                else:
                    st.write('N/A')
            
            with cols[4]:
                st.write(row.get('source', 'N/A'))
            
            with cols[5]:
                if st.button("❌", key=f"hide_table_{idx}", help="Hide this article"):
                    hide_news_row(idx)
                    st.rerun()
            
            # Add subtle divider between rows
            st.markdown('<hr style="margin: 5px 0; opacity: 0.3;">', unsafe_allow_html=True)

        
