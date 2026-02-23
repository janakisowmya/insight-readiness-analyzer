import streamlit as st
import pandas as pd
from ira.profiling.profile import create_profile
 
def render_profile_view():
    """Render the Profiler tab."""
    df = st.session_state.df
    offline_profile = st.session_state.get("uploaded_profile")
    
    if df is None and offline_profile is None:
        st.info("Please upload a dataset or a Profile JSON in the 'Load Data' tab first.")
        return
 
    st.header("📊 Data Profile")
    
    profile = None
    
    # Pathway A: Real-time Profiling (Small Files)
    if df is not None:
        if st.button("Generate Profile"):
            with st.spinner("Analyzing data..."):
                # Use session policy or empty dict
                policy = st.session_state.get("policy") or {}
                
                # Run Core Engine Profiler
                profile = create_profile(df, policy)
                st.session_state.report = profile 
    
    # Pathway B: Offline Profile (Large Files)
    elif offline_profile is not None:
        st.caption("Viewing Offline Profile")
        profile = offline_profile
        
    # Render Report if available
    if profile:
        # Metrics Row
        meta = profile["metadata"]
        row_count = meta["row_count"]
        col_count = meta["col_count"]
        
        # Calculate total missing cells from columns
        # (profile doesn't have total_missing_cells in metadata, need to sum)
        total_cells = row_count * col_count
        total_missing = sum(int(c["effective_missing_pct"] * row_count) for c in profile["columns"].values())
        completeness = 100 * (1 - total_missing / total_cells) if total_cells > 0 else 0
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows", row_count)
        c2.metric("Columns", col_count)
        c3.metric("Missing Cells", total_missing)
        c4.metric("Completeness", f"{completeness:.1f}%")
        
        st.markdown("### Column Details")
        
        # Prepare table for display
        rows = []
        for col, stats in profile["columns"].items():
            missing_count = int(stats["effective_missing_pct"] * row_count)
            unique_count = int(stats["unique_pct"] * row_count)
            
            rows.append({
                "Column": col,
                "Type": stats["inferred_pandas_dtype"],
                "Missing": f"{missing_count} ({stats['effective_missing_pct']*100:.1f}%)",
                "Unique (Est)": unique_count,
                "Sample": str([x["value"] for x in stats["frequent_values"][:3]])
            })
        
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
        
        # Readiness Score
        score = profile.get("readiness", {}).get("score", 0)
        st.progress(score / 100, text=f"Readiness Score: {score}/100")
        
        st.success("Profile Loaded successfully!")
