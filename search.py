import streamlit as st
import pandas as pd
import database

def show():
    """
    Displays the search and dashboard view of pipelines.
    """
    st.subheader("🔍 Search Pipelines")

    # --- FIX: State management for delete confirmation ---
    # These lines must be at the very top of the function
    if 'delete_confirm' not in st.session_state:
        st.session_state.delete_confirm = None
    if 'pipeline_to_delete' not in st.session_state:
        st.session_state.pipeline_to_delete = None

    all_pipelines = database.get_all_pipelines()

    # --- Conditional setup for filter options ---
    if all_pipelines:
        df = pd.DataFrame(all_pipelines)
        status_options = ["All"] + sorted(df['IS_ACTIVE'].unique().tolist())
        layer_options = ["All"] + sorted(df['ETL_LAYER'].unique().tolist())
        all_units = sorted(df['BUSINESS_UNIT'].unique().tolist())
        unit_options = ["All"] + all_units
    else:
        df = pd.DataFrame()
        status_options = ["All"]
        layer_options = ["All"]
        unit_options = ["All"]

    # --- Render Filter Controls (Always visible) ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        search_query = st.text_input(
            "Search by name, ID, business unit, or owner...",
            placeholder="e.g., customer_sales_etl_001",
            key="search_query"
        )
    with col2:
        status_filter = st.selectbox("Statuses", status_options, key="status_filter")
    with col3:
        layer_filter = st.selectbox("Layers", layer_options, key="layer_filter")
    with col4:
        unit_filter = st.selectbox("Units", unit_options, key="unit_filter")
    
    # --- Filter data based on selections ---
    if not df.empty:
        filtered_df = df.copy()
        if search_query:
            filtered_df = filtered_df[filtered_df.apply(lambda row: search_query.lower() in str(row).lower(), axis=1)]
        if status_filter != "All":
            filtered_df = filtered_df[filtered_df['IS_ACTIVE'] == status_filter]
        if layer_filter != "All":
            filtered_df = filtered_df[filtered_df['ETL_LAYER'] == layer_filter]
        if unit_filter != "All":
            filtered_df = filtered_df[filtered_df['BUSINESS_UNIT'] == unit_filter]

        st.write(f"Pipeline Results ({len(filtered_df)} of {len(all_pipelines)} pipelines)")

        # --- Confirmation Pop-up for Delete (NEW) ---
        if st.session_state.delete_confirm:
            st.warning(f"Are you sure you want to delete pipeline '{st.session_state.pipeline_to_delete}'? This action cannot be undone.")
            col_confirm, col_cancel, _, _ = st.columns(4)
            with col_confirm:
                if st.button("Confirm Delete", key="confirm_delete"):
                    if database.delete_pipeline(st.session_state.pipeline_to_delete):
                        st.success(f"Pipeline '{st.session_state.pipeline_to_delete}' deleted successfully!")
                    else:
                        st.error("Error deleting pipeline.")
                    st.session_state.delete_confirm = None
                    st.session_state.pipeline_to_delete = None
                    st.rerun()
            with col_cancel:
                if st.button("Cancel", key="cancel_delete"):
                    st.session_state.delete_confirm = None
                    st.session_state.pipeline_to_delete = None
                    st.rerun()
            with _:
                pass
            with _:
                pass
        # --- Display results with action buttons (Manual Row Rendering) ---
        if not filtered_df.empty:
            col_names = ["Pipeline", "Status", "Layer", "Business Unit", "Last Updated", "Actions"]
            cols = st.columns([5, 2, 1, 2, 2, 1])
            for col, col_name in zip(cols, col_names):
                col.markdown(f"**{col_name}**")
            st.markdown("---")

            for _, row in filtered_df.iterrows():
                cols = st.columns([5, 2, 1, 2, 2, 1])
                pipeline_name = row.get('DATA_FLOW_GROUP_ID', 'N/A')
                pipeline_status = "🟢 Active" if row.get('IS_ACTIVE') == 'Y' else "🔴 Inactive"
                pipeline_layer = row.get('ETL_LAYER', 'N/A')
                business_unit = row.get('BUSINESS_UNIT', 'N/A')
                updated_ts = row.get('UPDATED_TS', 'N/A')

                with cols[0]:
                    st.write(pipeline_name)
                with cols[1]:
                    st.write(pipeline_status)
                with cols[2]:
                    st.write(pipeline_layer)
                with cols[3]:
                    st.write(business_unit)
                with cols[4]:
                    st.write(updated_ts)
                with cols[5]:
                    if st.button("📝Edit", key=f"edit_{pipeline_name}"):
                        st.session_state.edit_pipeline_id = pipeline_name
                        st.session_state.current_pipeline_layer = pipeline_layer
                        st.session_state.form_visible = True
                        st.session_state.current_view = 'add_edit'
                        st.rerun()
                st.markdown("---")
        else:
            st.info("No pipelines match the current filters.")
    else:
        st.info("No pipelines found. Create one to get started!")