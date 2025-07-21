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

        # Sort by priority if available
        if 'priority_score' in filtered_df.columns:
            filtered_df = filtered_df.sort_values("priority_score", ascending=False)

        VISIBLE_COLS = [
            "title", "category", "published_at", "tickers", "url", "source","fund_names", "activists"
        ]
        existing_cols = [col for col in VISIBLE_COLS if col in filtered_df.columns]
        display_df = filtered_df[existing_cols].copy()
        # Display results
        st.dataframe(
            filtered_df.reset_index(drop=True),
            height=600,
            use_container_width=True,
            column_config={
                "url": st.column_config.LinkColumn("Article"),
                "published_at": st.column_config.DatetimeColumn("Published"),
                "tickers": st.column_config.ListColumn("Tickers"),
                "fund_names": st.column_config.ListColumn("Funds"),
                "activists": st.column_config.ListColumn("Activists"),
                "category": st.column_config.TextColumn("Category"),
                "source": st.column_config.TextColumn("Source"),
                "title": st.column_config.TextColumn("Title", width="large"),
            },
            column_order=existing_cols,
        )
