import streamlit as st
import pandas as pd
import yaml

def render_policy_editor():
    """Render the Policy Editor tab."""
    try:
        from ira.profiling.infer_policy import infer_policy
    except ImportError:
        st.error("Could not import infer_policy")
        return

    df = st.session_state.df
    offline_profile = st.session_state.get("uploaded_profile")
    
    if df is None and offline_profile is None:
        st.info("Please upload a dataset or profile first.")
        return

    # Determine Columns source
    if df is not None:
        columns = df.columns
    else:
        columns = list(offline_profile.get("columns", {}).keys())

    st.header("🔧 Policy Editor")
    st.markdown("Define how IRA should clean your data.")
    
    col_tools1, col_tools2 = st.columns(2)
    
    # 0. Upload Policy
    uploaded_policy = col_tools1.file_uploader("📂 Upload Policy YAML", type=["yaml", "yml"])
    if uploaded_policy is not None:
        try:
            # Only load if it's a new upload or different to current
            # Streamlit re-runs script on upload, so we can just load it.
            # We need to be careful not to overwrite if they are editing it.
            # Simple logic: If uploaded file changes, reload.
            # We can use session state to track loaded file name.
            if st.session_state.get("loaded_policy_file") != uploaded_policy.name:
                p_data = yaml.safe_load(uploaded_policy)
                if isinstance(p_data, dict):
                    st.session_state.policy = p_data
                    st.session_state.loaded_policy_file = uploaded_policy.name
                    st.toast("Policy loaded from YAML!", icon="📂")
                    # Rerun to refresh the view immediately
                    st.rerun()
        except Exception as e:
            st.error(f"Error loading YAML: {e}")

    # 1. Download/Export
    if st.session_state.policy:
        col_tools2.download_button(
            label="💾 Download Policy YAML",
            data=yaml.dump(st.session_state.policy, sort_keys=False),
            file_name="policy.yaml",
            mime="application/x-yaml",
            help="Download for use with CLI",
            use_container_width=True
        )

    # 2. Init Policy (if empty and no upload)
    if st.session_state.policy is None:
        st.info("No policy active. Upload a YAML above, or auto-infer below.")
        
        if df is not None:
            if st.button("🤖 Auto-Infer Policy from Data", type="primary"):
                with st.spinner("Inferring rules..."):
                    try:
                        policy = infer_policy(df)
                        st.session_state.policy = policy
                        st.success("Policy inferred! You can now edit it below.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Inference failed: {e}")
        else:
             if st.button("Initialize Empty Policy"):
                st.session_state.policy = {"dataset": {"name": "offline"}, "parsing": {}, "imputation": {}, "roles": {}}
                st.rerun()
        return

    policy = st.session_state.policy
    
    def save(): st.session_state.policy = policy
    
    # 3. Raw Editor (for power users who want to "Edit it over there")
    with st.expander("📝 Raw YAML Editor (Advanced)", expanded=False):
        st.caption("Edit the raw policy YAML directly. Changes apply on 'Ctrl+Enter'.")
        policy_str = yaml.dump(policy, sort_keys=False)
        new_policy_str = st.text_area("Policy YAML", value=policy_str, height=300)
        
        if new_policy_str != policy_str:
             try:
                 new_policy = yaml.safe_load(new_policy_str)
                 if isinstance(new_policy, dict):
                     st.session_state.policy = new_policy
                     st.toast("Policy updated from raw editor!", icon="💾")
                     # We might want to rerun to update the grid, but text_area triggers rerun on change usually if entered?
                     # Let's verify. 'On change' reruns.
                     save()
             except Exception as e:
                 st.error(f"Invalid YAML: {e}")

    # 4. Global Strategy
    with st.expander("🌍 Global Settings", expanded=False):
        imp = policy.get("imputation", {})
        current_strat = imp.get("numeric_strategy", "mean")
        new_strat = st.selectbox(
            "Default Numeric Strategy", 
            ["mean", "median", "mode", "constant"],
            index=["mean", "median", "mode", "constant"].index(current_strat) if current_strat in ["mean", "median", "mode", "constant"] else 0
        )
        if new_strat != current_strat:
            if "imputation" not in policy: policy["imputation"] = {}
            policy["imputation"]["numeric_strategy"] = new_strat
            save()

    # 3. MASTER CONFIGURATION GRID
    st.divider()
    st.subheader("📋 Column Configuration Grid")
    
    # Rebuild logic ...
    parsing = policy.get("parsing", {})
    col_types = parsing.get("column_types", {})
    imputation = policy.get("imputation", {})
    impute_cols = set(imputation.get("columns", []))
    roles = policy.get("roles", {})
    crit_cols = set(roles.get("critical_columns", []))
    prot_cols = set(roles.get("protected_columns", []))
    
    rows = []
    for col in columns:
        rows.append({
            "Column Name": col,
            "Type": col_types.get(col, "inferred"),
            "✨ Impute?": col in impute_cols,
            "🚨 Critical?": col in crit_cols,
            "🔒 Protected?": col in prot_cols
        })
        
    config_df = pd.DataFrame(rows)
    
    edited_df = st.data_editor(
        config_df,
        column_config={
            "Column Name": st.column_config.TextColumn(disabled=True),
            "Type": st.column_config.SelectboxColumn(
                options=["inferred", "numeric", "integer", "float", "boolean", "datetime", "text"],
                required=True
            ),
            "✨ Impute?": st.column_config.CheckboxColumn(
                help="If checked, missing values in this column will be FILLED using the strategy (e.g., mean/median)."
            ),
            "🚨 Critical?": st.column_config.CheckboxColumn(
                help="If checked, rows with MISSING values in this column will be DROPPED entirely. Use for mandatory fields like IDs."
            ),
            "🔒 Protected?": st.column_config.CheckboxColumn(
                help="If checked, data in this column will be MASKED (e.g., ***) in reports to protect PII."
            ),
        },
        hide_index=True,
        use_container_width=True,
        key="policy_grid"
    )
    
    # Apply changes
    new_impute = []
    new_crit = []
    new_prot = []
    new_types = {}
    
    for _, row in edited_df.iterrows():
        col_name = row["Column Name"]
        if row["✨ Impute?"]: new_impute.append(col_name)
        if row["🚨 Critical?"]: new_crit.append(col_name)
        if row["🔒 Protected?"]: new_prot.append(col_name)
        if row["Type"] != "inferred" and row["Type"] != "text":
            new_types[col_name] = row["Type"]
            
    if "imputation" not in policy: policy["imputation"] = {}
    policy["imputation"]["columns"] = sorted(new_impute)
    
    if "roles" not in policy: policy["roles"] = {}
    policy["roles"]["critical_columns"] = sorted(new_crit)
    policy["roles"]["protected_columns"] = sorted(new_prot)
    
    if "parsing" not in policy: policy["parsing"] = {}
    policy["parsing"]["column_types"] = new_types
    save()

    # 4. NUMERIC RANGES (Dropdown Selector)
    st.divider()
    st.subheader("📏 Numeric Range Check")
    
    validity = policy.get("validity_rules", {})
    
    # Improved Detection: Include columns that are explictly numeric OR look numeric
    numeric_candidates = []
    
    for c in columns:
        # 1. Explicitly set to numeric/int/float in UI
        user_type = new_types.get(c, "inferred") 
        if user_type in ["numeric", "integer", "float"]:
            numeric_candidates.append(c)
            continue
            
        # 2. Inferred as text, but pandas thinks it's numeric (Only if DF is available)
        if df is not None:
            if user_type == "inferred" and pd.api.types.is_numeric_dtype(df[c]):
                numeric_candidates.append(c)
                continue

            # 3. Inferred as text, but might be dirty numbers (check sample)
            if user_type == "inferred" and df[c].dtype == object:
                 # Try converting sample
                 sample = df[c].dropna().head(20)
                 if len(sample) > 0:
                     try:
                         pd.to_numeric(sample)
                         # If successful, it's a candidate
                         numeric_candidates.append(c)
                     except (ValueError, TypeError):
                         pass
                         
        # 4. Offline Fallback (Check profile types)
        elif offline_profile:
             # If profile says it's numeric, add it
             p_cols = offline_profile.get("columns", {})
             if c in p_cols:
                 p_type = p_cols[c].get("type", "unknown")
                 if p_type in ["numeric", "integer", "float"]:
                     numeric_candidates.append(c)

    numeric_cols = sorted(list(set(numeric_candidates)))

    if not numeric_cols:
         st.warning("No numeric columns detected automatically.")
         st.info("💡 **Tip:** If a column is missing here, go to the **Configuration Grid** above and change its 'Type' to **Numeric**.")
    else:
        st.caption("Select a column to define minimum and maximum valid values.")
        selected_num_col = st.selectbox("Select Numeric Column to Constrain", options=numeric_cols)
        
        if selected_num_col:
            st.markdown(f"**Settings for `{selected_num_col}`**")
            col = selected_num_col
            col_rules = validity.get(col, {})
            cur_min = col_rules.get("min_value")
            cur_max = col_rules.get("max_value")
            
            # Safe stats
            if df is not None:
                num_series = pd.to_numeric(df[col], errors='coerce')
                if num_series.notna().any():
                        stats = num_series.describe()
                        st.caption(f"Min: {stats['min']:.2f} | Max: {stats['max']:.2f} | Mean: {stats['mean']:.2f}")
                else:
                        st.caption("No numeric data found.")
            else:
                # Offline stats from profile
                p_cols = offline_profile.get("columns", {}) if offline_profile else {}
                if col in p_cols and "stats" in p_cols[col]:
                    stats = p_cols[col]["stats"]
                    # Profile structure varies, usually keys are "min", "max", "mean"
                    s_min = stats.get("min")
                    s_max = stats.get("max")
                    s_mean = stats.get("mean")
                    if s_min is not None:
                         st.caption(f"Min: {s_min} | Max: {s_max} | Mean: {s_mean} (from Profile)")
            
            c1, c2, c3, c4 = st.columns([1, 2, 1, 2])
            use_min = c1.checkbox("Min", value=(cur_min is not None), key=f"chk_min_{col}")
            new_min = c2.number_input("Val", value=float(cur_min) if cur_min is not None else 0.0, key=f"n_min_{col}", disabled=not use_min)
            
            use_max = c3.checkbox("Max", value=(cur_max is not None), key=f"chk_max_{col}")
            new_max = c4.number_input("Val", value=float(cur_max) if cur_max is not None else 100.0, key=f"n_max_{col}", disabled=not use_max)

            # Update Validity
            if st.button(f"Save Range for {col}"):
                if col not in validity: validity[col] = {}
                
                # Ensure validity rule defaults to "null" (so data gets cleaned)
                if "on_violation" not in validity:
                    validity["on_violation"] = "null"
                
                if use_min: validity[col]["min_value"] = new_min
                elif "min_value" in validity[col]: validity[col].pop("min_value")
                    
                if use_max: validity[col]["max_value"] = new_max
                elif "max_value" in validity[col]: validity[col].pop("max_value")

                if not validity[col]: validity.pop(col)
                
                policy["validity_rules"] = validity
                save()
                st.success(f"Saved range for {col}!")
