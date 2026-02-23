import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any

def init_session_state():
    """Initialize Streamlit session state variables if they don't exist."""
    defaults = {
        "df": None,              # The raw dataframe
        "filename": None,        # Name of the uploaded file
        "policy": None,          # The cleaning policy (dict)
        "clean_df": None,        # The cleaned dataframe
        "audit_log": None,       # The audit log (list of dicts)
        "report": None,          # The quality report
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def reset_state():
    """Clear all state."""
    st.session_state.clear()
    init_session_state()
