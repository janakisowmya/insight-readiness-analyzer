import streamlit as st
import pandas as pd
import time
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.audit import AuditLogger

def render_correction_view():
    """Render the Correction/Execution tab."""
    st.header("⚡ Run Correction Engine")
    
    # Check prerequisites
    if st.session_state.df is None:
        st.info("Please upload a dataset first (Tab 1).")
        return
        
    if st.session_state.policy is None:
        st.warning("No policy defined. Please go to the 'Policy' tab and infer/edit a policy first.")
        return

    st.markdown("Ready to clean your data using the defined policy.")
    
    # Display summary of config
    with st.expander("Review Configuration", expanded=True):
        policy = st.session_state.policy
        n_impute = len(policy.get("imputation", {}).get("columns", []))
        n_critical = len(policy.get("roles", {}).get("critical_columns", []))
        n_validity = len(policy.get("validity_rules", {}))
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Imputation Fields", n_impute)
        c2.metric("Critical Fields", n_critical)
        c3.metric("Validity Rules", n_validity)

    if st.button("🚀 Run Repair Pipeline", type="primary"):
        progress_bar = st.progress(0, text="Starting engine...")
        # Save audit log to a local file for now
        audit_path = "audit_log.json"
        
        # Check Large File Mode
        is_preview = getattr(st.session_state, 'is_sample', False)
        # We need the real path for chunked processing
        # In upload mode, st.file_uploader doesn't give a path unless we saved it.
        # But we added "Local File Path", so we might have it.
        # If uploaded via browser, we can't do chunked processing on the original file easily unless we saved it to disk first.
        # But typically browser uploads are <200MB so in-memory is fine.
        # The key scenario is: "Local File Path" input was used -> we have st.session_state.filename equal to basename, 
        # do we store the full path? We need to check app.py storage.
        # Wait, app.py stored `fname` and `fsize`. It didn't store the full local path in session state explicitly 
        # other than maybe indirectly.
        # Let's assume we can get it from the widget key or we need to update app.py to store `source_path`.
        
        # HACK: For now, if local_path input widget has value, use it.
        # But widgets aren't always in session_state dict if not key'ed? 
        # Actually app.py: local_path = st.text_input(...) -> this is a variable in run script.
        # We need to access it. 
        # Let's first assume we can only do this if we can find the file.
        # Or we rely on the fact that if it's > 200MB, the user MUST have used local path per our instructions.
        
        # We'll use a session state var 'source_path' which we should have set in app.py. 
        # I'll need to update app.py to set this. For now let's write the CONSUMER code here assuming it exists.
        
        source_path = st.session_state.get("source_path")
        
        try:
            # 1. Setup
            progress_bar.progress(10, text="Initializing pipeline...")
            time.sleep(0.3)
            
            if is_preview and source_path:
                st.info(f"🚀 Running Large File Pipeline on `{source_path}`...")
                from ira.correction.pipeline import run_chunked_correction
                
                # Output path
                out_path = f"cleaned_{st.session_state.filename}"
                
                # Setup UI Callback
                def ui_progress(chunk_idx, total_est):
                     p = min(0.95, chunk_idx / (total_est if total_est > 0 else 1))
                     progress_bar.progress(p, text=f"Processing Chunk {chunk_idx}...")
                     
                audit = AuditLogger(audit_path, detail="summary")
                try:
                    total_rows = run_chunked_correction(
                        input_path=source_path,
                        output_path=out_path,
                        ChunkSize=50000, # Conservative chunk size for UI
                        policy=policy,
                        audit=audit,
                        progress_callback=ui_progress
                    )
                    st.session_state.cleaned_file_path = out_path
                    st.session_state.audit_stats = audit.get_stats_df()
                    st.session_state.audit_samples = audit.get_logs() # might be empty if summary
                    
                finally:
                    audit.close()
                    
                progress_bar.progress(100, text="Done!")
                st.success(f"🎉 Large File Cleaned! Saved to `{out_path}`")
                st.info(f"Processed {total_rows} rows.")
                
                # We can't load the full result into RAM for display, but we can load a sample
                st.caption("Loading sample of cleaned data...")
                st.session_state.cleaned_df = pd.read_csv(out_path, nrows=1000)
                
            else:
                # Standard In-Memory
                audit = AuditLogger(audit_path)
                
                # Ensure _row_id exists (required by pipeline)
                working_df = st.session_state.df.copy()
                if "_row_id" not in working_df.columns:
                    working_df.insert(0, "_row_id", range(len(working_df)))
                
                # 2. Execution
                progress_bar.progress(30, text="Standardizing and Parsing...")
                
                cleaned_df = run_correction_pipeline(
                    df=working_df,
                    policy=policy,
                    audit=audit
                )
                
                progress_bar.progress(80, text="Finalizing imputation...")
                time.sleep(0.2)
                
                # 3. Store Results
                audit.close()
                st.session_state.cleaned_df = cleaned_df
                st.session_state.audit_stats = audit.get_stats_df()
                st.session_state.audit_samples = audit.get_logs()
                
                progress_bar.progress(100, text="Done!")
                time.sleep(0.5)
                progress_bar.empty()
                
                st.success("🎉 Data Successfully Cleaned!")
            
            # Common Stats Display
            st.divider()
            c1, c2 = st.columns(2)
            
            # If large file, we might not have original rows count in memory easily unless we check file line count
            # or rely on what we stored.
            if is_preview:
                 c1.metric("Original Rows (Total)", "Unknown (Large File)")
                 c2.metric("Clean Rows", total_rows if 'total_rows' in locals() else "N/A")
            else:
                rows_original = len(st.session_state.df)
                rows_clean = len(st.session_state.cleaned_df)
                dropped = rows_original - rows_clean
                
                c1.metric("Original Rows", rows_original)
                c2.metric("Clean Rows", rows_clean, delta=f"-{dropped} dropped" if dropped > 0 else "No drops")
            
            # Audit Summary
            st.subheader("📝 Audit Summary")
            
            stats_df = st.session_state.audit_stats
            if not stats_df.empty:
                high_level = stats_df.groupby("Event Type")["Count"].sum().reset_index()
                st.dataframe(high_level, hide_index=True, use_container_width=True)
                
                with st.expander("View Detailed Stats"):
                    st.dataframe(stats_df, hide_index=True)

                if "audit_samples" in st.session_state:
                     with st.expander("View Request Log Samples"):
                         st.dataframe(pd.DataFrame(st.session_state.audit_samples))
            else:
                st.info("No modifications were logged. Data was already clean!")
                
        except Exception as e:
            st.error(f"Pipeline failed: {e}")
            import traceback
            st.code(traceback.format_exc())

