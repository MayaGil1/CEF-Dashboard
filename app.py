import streamlit as st
import sys
import os
from pathlib import Path



# Add panels to path
current_dir = Path(__file__).parent
panels_dir = current_dir / "panels"
sys.path.insert(0, str(panels_dir))

# Import panels
try:
    from panels import news
    from panels import sec_filings
    from panels import discounts  # Now properly implemented
    # Import other existing panels
    # import performance
except ImportError as e:
    st.error(f"Error importing panels: {e}")

def main():
    st.set_page_config(
        page_title="CEF Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📊 CEF Investment Dashboard")
    st.markdown("---")
    
    # Sidebar navigation
    with st.sidebar:
        st.title("Navigation")
        
        panel_choice = st.selectbox(
            "Choose Panel:",
            [
                "📰 News & Announcements",
                "📋 SEC Filings Monitor",
                "💰 Discount Analysis",
            ]
        )
    
    # Render selected panel
    if panel_choice == "📰 News & Announcements":
        news.render()
    elif panel_choice == "📋 SEC Filings Monitor":
        sec_filings.render()
    elif panel_choice == "💰 Discount Analysis":
        discounts.render()

if __name__ == "__main__":
    main()
