import streamlit as st
import uuid
import database
import datetime
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

def get_select_box_index(options, value):
    """Safely gets the index of a value in a list of options."""
    if value and value in options:
        return options.index(value)
    return 0

def update_etl_layer():
    """Callback function to synchronize the ETL Layer selectbox with the pipeline type and reset fields."""
    selected_layer_text = st.session_state.layer_selector
    layer_code = selected_layer_text.split(" ")[0]
    st.session_state['current_pipeline_layer'] = layer_code
    st.session_state['general_etl_layer'] = layer_code

    # Reset L0/L1/L2-specific fields if switching
    if layer_code in ["L1", "L2"]:
        st.session_state['num_tables'] = 1
        st.session_state['l0_tables_data'] = [{}]
    if layer_code == "L0":
        st.session_state['pb_data'] = {}


def load_help_texts(file_path):
    """Loads help texts from a specified text file."""
    help_texts = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split(':', 1)
                    help_texts[key.strip()] = value.strip()
    except FileNotFoundError:
        st.error(f"Help text file not found at: {file_path}")
    return help_texts

HELP_CONTENT = load_help_texts('info_context.txt')

def show(prefill_data=None):
    """
    Main function to display the application UI for creating and editing pipelines.
    Handles data pre-filling from the AI assistant or database.
    """
    # --- 1. Session State Initialization & Data Structure Setup ---
    # Initialize all necessary keys to avoid KeyErrors
    if 'form_visible' not in st.session_state:
        st.session_state['form_visible'] = False
    if 'current_pipeline_layer' not in st.session_state:
        st.session_state['current_pipeline_layer'] = "L0"
    if 'edit_pipeline_id' not in st.session_state:
        st.session_state['edit_pipeline_id'] = None
    if 'general_data' not in st.session_state:
        st.session_state['general_data'] = {}
    if 'l0_tables_data' not in st.session_state:
        st.session_state['l0_tables_data'] = [{}]
    if 'num_tables' not in st.session_state:
        st.session_state['num_tables'] = 1
    if 'pb_data' not in st.session_state:
        st.session_state['pb_data'] = {}

    
# --- 2. Load Data from AI or Database & Pre-fill Session State ---
    if prefill_data:
        st.session_state.form_visible = True
        st.session_state['general_data'] = prefill_data.get('header', {})
        
        # CRITICAL: Determine the actual layer from the header data
        layer = st.session_state['general_data'].get('ETL_LAYER', 'L0')
        # CRITICAL: This session state key must be updated and used by the form rendering logic
        st.session_state['current_pipeline_layer'] = layer
        
        if layer == 'L0':
            # For L0, load a list of source details
            st.session_state['l0_tables_data'] = prefill_data.get('detail', [{}])
            st.session_state['num_tables'] = len(st.session_state['l0_tables_data'])
            # Ensure L1/L2 data is explicitly cleared
            st.session_state['pb_data'] = {} 
        else: # L1 or L2
            # Ensure L0 data is explicitly cleared
            st.session_state['l0_tables_data'] = [{}]
            st.session_state['num_tables'] = 1
            
            # Safely retrieve the 'detail' list, defaulting to an empty list if not found.
            detail_list = prefill_data.get('detail', [])

            # Check if the list is empty. If it is, use an empty dictionary {} as the default.
            if detail_list:
                st.session_state['pb_data'] = detail_list[0]
            else:
                # This handles the case where the AI Assistant provided 'detail': []
                st.session_state['pb_data'] = {}
                
        st.info(f"AI data for **{layer}** pipeline loaded. You can edit it now. 🤖")    
    elif st.session_state.edit_pipeline_id:
        current_pipeline = database.get_pipeline_by_id(st.session_state.edit_pipeline_id)
        if not current_pipeline:
            st.error("Pipeline not found. 😔")
            st.session_state.form_visible = False
            st.session_state.edit_pipeline_id = None
            st.rerun()
        
        st.session_state.form_visible = True
        
        # Prefill General Data for ALL Layers
        st.session_state['general_data'] = current_pipeline
        
        layer = st.session_state['general_data'].get('ETL_LAYER', 'L0')
        st.session_state['current_pipeline_layer'] = layer

        # Prefill L0 Data
        if layer == "L0":
            l0_details_list = current_pipeline.get('l0_details', [])
            
            # This is the key change: pre-fill the l0_tables_data list
            st.session_state['l0_tables_data'] = l0_details_list if l0_details_list else [{}]
            st.session_state['num_tables'] = len(st.session_state['l0_tables_data'])
        
        # Prefill L1/L2 Data
        else:
            pb_details_values = current_pipeline.get('pb_details', [])
            # This is the key change: pre-fill the pb_data dictionary
            st.session_state['pb_data'] = pb_details_values[0] if isinstance(pb_details_values, list) and pb_details_values else {}
        
        st.info("Pipeline data loaded from database. You can edit it now. 👍")
        
    if st.session_state.form_visible:
        if st.button("← Back to Dashboard"):
            st.session_state.form_visible = False
            st.session_state.edit_pipeline_id = None
            st.rerun()
            
        # st.subheader("Create/Edit Pipeline Metadata")
        # layer_options = ["L0 Raw Layer", "L1 Curated Layer", "L2 Data Product Layer"]
        # selected_layer_text = st.session_state.get('current_pipeline_layer', 'L0')
        # selected_layer_index = get_select_box_index([item.split(' ')[0] for item in layer_options], selected_layer_text)

        # st.selectbox(
        #     "Select Pipeline Type/Layer",
        #     layer_options,
        #     index=selected_layer_index,
        #     key='layer_selector',
        #     on_change=update_etl_layer,
        #     disabled=bool(st.session_state.edit_pipeline_id)
        # )
        # current_layer = st.session_state.current_pipeline_layer
        
        st.subheader("Create/Edit Pipeline Metadata")
        
        # 1. Define Layer Mappings and Options
        LAYER_MAPPING = {
            'L0': 'L0 Raw Layer',
            'L1': 'L1 Curated Layer',
            'L2': 'L2 Data Product Layer'
        }
        layer_options = list(LAYER_MAPPING.values())
        
        # 2. Get the current layer code from session state (e.g., 'L1')
        current_layer_code = st.session_state.get('current_pipeline_layer', 'L0')
        
        # 3. CRITICAL: Map the current layer code to its full display label
        initial_layer_label = LAYER_MAPPING.get(current_layer_code, LAYER_MAPPING['L0'])

        # 4. CRITICAL FIX: Calculate the index of the full label in the options list
        # Your helper function is not used correctly here because it's meant to search codes,
        # but we need the index of the full label. We can get this directly from the list.
        try:
            selected_layer_index = layer_options.index(initial_layer_label)
        except ValueError:
            selected_layer_index = 0 # Default to L0 if the mapped label isn't found
        
        # The line below is the original line, which is replaced by the logic above.
        # selected_layer_text = st.session_state.get('current_pipeline_layer', 'L0')
        # selected_layer_index = get_select_box_index([item.split(' ')[0] for item in layer_options], selected_layer_text)

        st.selectbox(
            "Select Pipeline Type/Layer",
            layer_options,
            index=selected_layer_index, # <-- This is now the index of 'L1 Curated Layer'
            key='layer_selector',
            on_change=update_etl_layer,
            disabled=bool(st.session_state.edit_pipeline_id)
        )
        current_layer = st.session_state.current_pipeline_layer
        # --- General Configuration Section ---
        with st.container(border=True):
            st.subheader("General Information")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.text_input("Dataflow Name *", value=st.session_state['general_data'].get('DATA_FLOW_GROUP_ID', ''), disabled=bool(st.session_state.edit_pipeline_id), key="general_data_flow_id", placeholder='MASTER_DATA_INDUSTRY_BUSINESS_L0', help=HELP_CONTENT.get("DATAFLOW_NAME_HELP", ""))
                st.text_input("Business Unit *", value=st.session_state['general_data'].get('BUSINESS_UNIT', ''), key="general_business_unit", placeholder='sales', help=HELP_CONTENT.get("BUSINESS_UNIT_HELP", ""))
                st.text_input("Product Owner *", value=st.session_state['general_data'].get('PRODUCT_OWNER', ''), placeholder="name or mail_id", key="general_product_owner", help=HELP_CONTENT.get("PRODUCT_OWNER_HELP", ""))
                status_options = ["Y", "N"]
                st.selectbox("Status *", status_options, index=get_select_box_index(status_options, st.session_state['general_data'].get("IS_ACTIVE", "Y")), key="general_is_active", help=HELP_CONTENT.get("STATUS_HELP", ""))
                st.number_input("Warning threshold(minutes) *", min_value=0, value=st.session_state['general_data'].get('WARNING_THRESHOLD_MINS', 30), key="general_warning_threshold", help=HELP_CONTENT.get("WARNING_THRESHOLD_HELP", ""))
            with col2:
                if current_layer == "L0":
                    st.text_input("Trigger type *", value="DLT", disabled=True)
                    st.selectbox("ETL Layer *", ["L0"], index=0, key="general_etl_layer", disabled=True)
                else:
                    trigger_options = ["DLT", "JOB"]
                    st.selectbox("Trigger type *", trigger_options, index=get_select_box_index(trigger_options, st.session_state['general_data'].get('TRIGGER_TYPE', "JOB")), key="general_trigger_type", help=HELP_CONTENT.get("TRIGGER_TYPE_HELP", ""))
                    etl_layer_options = ["L1", "L2"]
                    st.selectbox("ETL Layer *", etl_layer_options, index=get_select_box_index(etl_layer_options, current_layer), key="general_etl_layer", disabled=True)
                
                st.text_input("Data SME *", value=st.session_state['general_data'].get('DATA_SME', ''), key="general_data_sme", placeholder='DATA_SME', help=HELP_CONTENT.get("DATA_SME_HELP", ""))
                st.text_input("Cost center", value=st.session_state['general_data'].get('COST_CENTER', ''), key="general_cost_center", placeholder='COST_CENTER_NAME', help=HELP_CONTENT.get("COST_CENTER_HELP", ""))
                st.text_input("Warning DL group *", value=st.session_state['general_data'].get('WARNING_DL_GROUP', ''), key="general_warning_dl_group", placeholder='ted_simplification_data_team', help=HELP_CONTENT.get("WARNING_DL_GROUP_HELP", ""))
            with col3:
                st.text_input("Business Object Name", value=st.session_state['general_data'].get('BUSINESS_OBJECT_NAME', ''), key="general_business_object_name", placeholder='Daily_news', help=HELP_CONTENT.get("BUSINESS_OBJECT_NAME_HELP", ""))
                compute_class_options = database.get_compute_classes(dev_allowed=False)
                st.selectbox("Compute class *", compute_class_options, index=get_select_box_index(compute_class_options, st.session_state['general_data'].get('COMPUTE_CLASS')), key="general_compute_class", help=HELP_CONTENT.get("COMPUTE_CLASS_HELP", ""))
                dev_compute_class_options = database.get_compute_classes(dev_allowed=True)
                st.selectbox("Compute class Dev *", dev_compute_class_options, index=get_select_box_index(dev_compute_class_options, st.session_state['general_data'].get('COMPUTE_CLASS_DEV')), key="general_compute_class_dev", help=HELP_CONTENT.get("COMPUTE_CLASS_DEV_HELP", ""))
                st.number_input("Min version", value=st.session_state['general_data'].get('min_version', 0.10), key="general_min_version", help=HELP_CONTENT.get("MIN_VERSION_HELP", ""))
                st.number_input("Max version", value=st.session_state['general_data'].get('max_version', 0.10), key="general_max_version", help=HELP_CONTENT.get("MAX_VERSION_HELP", ""))
            with col4:
                st.text_input("Spark configs", value=st.session_state['general_data'].get('SPARK_CONFIGS', ''), key="general_spark_configs", placeholder='null', help=HELP_CONTENT.get("SPARK_CONFIGS_HELP", ""))
                st.text_input("Inserted By", value=st.session_state['general_data'].get('INSERTED_BY', ''), key="general_inserted_by", placeholder='current_user', help=HELP_CONTENT.get("INSERTED_BY_HELP", ""))
                st.text_input("Updated By", value=st.session_state['general_data'].get('UPDATED_BY', ''), key="general_updated_by", placeholder='current_user', help=HELP_CONTENT.get("UPDATED_BY_HELP", ""))
                
                is_disabled = current_layer != "L0"
                ingestion_mode_options = ["EXTL_FULL", "EXTL_INC", "DATASPHERE_INGEST", "API_INGEST", "DB_INGEST"]
                st.selectbox("Ingestion Mode *", ingestion_mode_options, index=get_select_box_index(ingestion_mode_options, st.session_state['general_data'].get('INGESTION_MODE', 'EXTL_FULL')), key="general_ingestion_mode", disabled=is_disabled, help=HELP_CONTENT.get("INGESTION_MODE_HELP", ""))
                st.text_input("Ingestion bucket *", value=st.session_state['general_data'].get('INGESTION_BUCKET', '') if not is_disabled else "", key="general_ingestion_bucket", disabled=is_disabled, placeholder="onedata", help=HELP_CONTENT.get("INGESTION_BUCKET_HELP", ""))
        
        if current_layer == "L0":
            st.header("Source Configuration")
            st.info("Source, Source Schema, and Source Object Name for each table must also be unique.")
            
            num_tables = st.number_input(
                "Number of Source Tables",
                min_value=1,
                max_value=5,
                value=st.session_state.get('num_tables', 1),
                key='l0_num_tables_input'
            )

            while len(st.session_state['l0_tables_data']) < num_tables:
                st.session_state['l0_tables_data'].append({})
            st.session_state['l0_tables_data'] = st.session_state['l0_tables_data'][:num_tables]
            
            for i in range(num_tables):
                with st.expander(f"Table {i+1}", expanded=True):
                    with st.container(border=True):
                        st.subheader("Source Configuration")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.text_input("Source *", value=st.session_state['l0_tables_data'][i].get('SOURCE', ''), key=f"source_{i}", placeholder='source_name', help=HELP_CONTENT.get("SOURCE_HELP", ""))
                            storage_options = ["C1", "C2", "C3", "C4"]
                            st.selectbox("Storage type *", storage_options, index=get_select_box_index(storage_options, st.session_state['l0_tables_data'][i].get('STORAGE_TYPE', "C1").upper()), key=f"storage_type_{i}", help=HELP_CONTENT.get("STORAGE_TYPE_HELP", ""))
                        with col2:
                            st.text_input("Source schema *", value=st.session_state['l0_tables_data'][i].get('SOURCE_OBJ_SCHEMA', ''), key=f"source_schema_{i}", placeholder='GBL_CURRENT', help=HELP_CONTENT.get("SOURCE_SCHEMA_HELP", ""))
                            file_format_options = ["parquet", "csv", "tsv", "json", "xml"]
                            st.selectbox("Input file format *", 
                                file_format_options, 
                                index=get_select_box_index(
                                    file_format_options, 
                                    (st.session_state['l0_tables_data'][i].get('INPUT_FILE_FORMAT') or "parquet").strip().lower()
                                ), 
                                key=f"input_file_format_{i}", 
                                help=HELP_CONTENT.get("INPUT_FILE_FORMAT_HELP", ""))
                        with col3:
                            st.text_input("Delimeter", value=st.session_state['l0_tables_data'][i].get('DELIMETER', ''), key=f"delimeter_{i}", placeholder='delimeter_name', help=HELP_CONTENT.get("DELIMETER_HELP", ""))
                            st.text_input("Source object name *", value=st.session_state['l0_tables_data'][i].get('SOURCE_OBJ_NAME', ''), key=f"source_obj_name_{i}", placeholder='gbl_industry_business_test', help=HELP_CONTENT.get("SOURCE_OBJ_NAME_HELP", ""))
                        with col4:
                            st.text_area("Custom schema", value=st.session_state['l0_tables_data'][i].get('CUSTOM_SCHEMA', ''), key=f"custom_schema_{i}", placeholder='custer_schema', help=HELP_CONTENT.get("CUSTOM_SCHEMA_HELP", ""))
                    
                    with st.container(border=True):
                        st.subheader("Data Quality & Processing")
                        col1, col2, col3, = st.columns(3)
                        with col1:
                            st.text_area("DQ Logic *", value=st.session_state['l0_tables_data'][i].get('DQ_LOGIC', ''), key=f"dq_logic_{i}", placeholder=''' no_rescued_data: _rescued_data IS NULL
                                                                 valid_id: _rescued_data IS NULL AND GIB_INDUSTRY_BUSINESS_CODE IS NOT NULL ...''', help=HELP_CONTENT.get("DQ_LOGIC_HELP", ""))
                        with col2:
                            st.text_area("CDC Logic *", value=st.session_state['l0_tables_data'][i].get('CDC_LOGIC', ''), placeholder='''apply_as_deletes: operation_column = 'DELETE'
                                                                 except_column_list: ["_rescued_data", "inputFilePath", ...]''', key=f"cdc_logic_{i}", help=HELP_CONTENT.get("CDC_LOGIC_HELP", ""))
                        with col3:
                            st.text_area("Transform query *", value=st.session_state['l0_tables_data'][i].get('TRANSFORM_QUERY', ''), key=f"transform_query_{i}", placeholder="map('GIBDDL_IND_BSNS_CODE', 'cast(GIBDDL_IND_BSNS_CODE as int)')", help=HELP_CONTENT.get("TRANSFORM_QUERY_HELP", ""))
                    
                    with st.container(border=True):
                        st.subheader("Target Configuration")
                        col1, col2, col3, = st.columns(3)
                        with col1:
                            load_type_options = ["FULL", "DELTA"]
                            st.selectbox("Load type *", load_type_options, index=get_select_box_index(load_type_options, st.session_state['l0_tables_data'][i].get('LOAD_TYPE', "FULL")), key=f"load_type_{i}", help=HELP_CONTENT.get("LOAD_TYPE_HELP", ""))
                            prestag_options = ["Y", "N"]
                            st.selectbox("Prestag flag *", prestag_options, index=get_select_box_index(prestag_options, st.session_state['l0_tables_data'][i].get('PRESTAG_FLAG', "Y")), key=f"prestag_flag_{i}", help=HELP_CONTENT.get("PRESTAG_FLAG_HELP", ""))
                        with col2:
                            st.text_input("LOB", value=st.session_state['l0_tables_data'][i].get('LOB', ''), placeholder='master-data', key=f"lob_{i}", help=HELP_CONTENT.get("LOB_HELP", ""))
                            st.text_input("Partition", value=st.session_state['l0_tables_data'][i].get('PARTITION', ''), key=f"partition_{i}", placeholder='null', help=HELP_CONTENT.get("PARTITION_HELP", ""))
                        with col3:
                            status_options = ["Y", "N"]
                            st.selectbox("Status *", status_options, index=get_select_box_index(status_options, st.session_state['l0_tables_data'][i].get('IS_ACTIVE', "Y")), key=f"status_{i}", help=HELP_CONTENT.get("STATUS_HELP", ""))

        if current_layer in ["L1", "L2"]:
            with st.container(border=True):
                st.subheader("Target configuration")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    target_obj_options = ["MV", "Table", "View"]
                    st.selectbox("Target object type *", target_obj_options, index=get_select_box_index(target_obj_options, st.session_state['pb_data'].get('TARGET_OBJ_TYPE', 'MV')), key="target_obj_type_L1_L2", help=HELP_CONTENT.get("TARGET_OBJ_TYPE_HELP", ""))
                    st.text_input("LOB *", value=st.session_state['pb_data'].get('LOB', ''), placeholder='master-data', key="lob_L1_L2")
                    load_type_options = ["FULL", "DELTA", "SCD", "PySpark"]
                    st.selectbox("Load type *", load_type_options, index=get_select_box_index(load_type_options, st.session_state['pb_data'].get('LOAD_TYPE', 'FULL')), key="load_type_L1_L2", help=HELP_CONTENT.get("LOAD_TYPE_HELP", ""))
                with col2:
                    st.text_input("Target schema *", value=st.session_state['pb_data'].get('TARGET_OBJ_SCHEMA', ''), placeholder='master_data_l1_curated', key="target_obj_schema_L1_L2", help=HELP_CONTENT.get("TARGET_SCHEMA_HELP", ""))
                    # priority_options = ["Low", "Medium", "High", "Critical"]
                    # st.selectbox("Priority *", priority_options, index=get_select_box_index(priority_options, st.session_state['pb_data'].get('PRIORITY', 'Medium')), key="priority_L1_L2", help=HELP_CONTENT.get("PRIORITY_HELP", ""))
                    st.number_input("Priority", value=st.session_state['pb_data'].get('PRIORITY', 0), key="priority_L1_L2", help=HELP_CONTENT.get("PRIORITY_HELP", ""))
                    is_disabled = st.session_state.get('target_obj_type_L1_L2') != 'Table'
                    st.text_input("Target PK", value=st.session_state['pb_data'].get('TARGET_PK', ''), placeholder='target_pk', key="target_pk_L1_L2", disabled=is_disabled)
                with col3:
                    st.text_input("Target name *", value=st.session_state['pb_data'].get('TARGET_OBJ_NAME', ''), placeholder='dimension_industry_business_current_test', key="target_obj_name_L1_L2", help=HELP_CONTENT.get("TARGET_NAME_HELP", ""))
                    st.text_input("Generic scripts", value=st.session_state['pb_data'].get('GENERIC_SCRIPTS', ''), key="generic_scripts_L1_L2", placeholder='generic_script', help=HELP_CONTENT.get("GENERIC_SCRIPTS_HELP", ""))
                    st.text_input("Source PK", value=st.session_state['pb_data'].get('SOURCE_PK', ''), key="source_pk_L1_L2", placeholder="source_pk", disabled=(st.session_state.get('target_obj_type_L1_L2') != 'Table'))
                with col4:
                    st.text_area("Transform query *", value=st.session_state['pb_data'].get('TRANSFORM_QUERY', ''), key="transform_query_L1_L2", placeholder='SELECT industry_business_code, industry_business_name, ...', help=HELP_CONTENT.get("TRANSFORM_QUERY_HELP", ""))
            
            with st.container(border=True):
                st.subheader("Advanced Configurations")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    partition_method_options = ["Partition", "Liquid cluster"]
                    st.selectbox("Partition method", partition_method_options, index=get_select_box_index(partition_method_options, st.session_state['pb_data'].get('PARTITION_METHOD', 'Partition')), key="partition_method_L1_L2", help=HELP_CONTENT.get("PARTITION_METHOD_HELP", ""))
                    st.text_input("Custom script parameters", value=st.session_state['pb_data'].get('CUSTOM_SCRIPT_PARAMS', ''), placeholder='Custom_script_parameters', key="custom_script_params_L1_L2", disabled=(st.session_state.get('target_obj_type_L1_L2') != 'Table'))
                with col2:
                    st.text_input("Partition or Index", value=st.session_state['pb_data'].get('PARTITION_OR_INDEX', ''), key="partition_or_index_L1_L2", placeholder="partition_method", help=HELP_CONTENT.get("PARTITION_OR_INDEX_HELP", ""))
                with col3:
                    status_options = ["Y", "N"]
                    st.selectbox("Status *", status_options, index=get_select_box_index(status_options, st.session_state['pb_data'].get('IS_ACTIVE', 'Y')), key="status_L1_L2", help=HELP_CONTENT.get("STATUS_HELP", ""))
                with col4:
                    st.text_input("Retention details", value=st.session_state['pb_data'].get('RETENTION_DETAILS', ''), key="retention_details_L1_L2", placeholder='0', disabled=(st.session_state.get('target_obj_type_L1_L2') != 'Table'))

        submitted = st.button("Save All Data", type="primary")

        if submitted:
            general_data = {
                "DATA_FLOW_GROUP_ID": st.session_state.get('general_data_flow_id'),
                "BUSINESS_UNIT": st.session_state.get('general_business_unit'),
                "PRODUCT_OWNER": st.session_state.get('general_product_owner'),
                "IS_ACTIVE": st.session_state.get('general_is_active'),
                "TRIGGER_TYPE": "DLT" if st.session_state.get('current_pipeline_layer') == "L0" else st.session_state.get('general_trigger_type'),
                "ETL_LAYER": st.session_state.get('general_etl_layer'),
                "DATA_SME": st.session_state.get('general_data_sme'),
                "INGESTION_MODE": st.session_state.get('general_ingestion_mode'),
                "INGESTION_BUCKET": st.session_state.get('general_ingestion_bucket'),
                "WARNING_THRESHOLD_MINS": st.session_state.get('general_warning_threshold'),
                "BUSINESS_OBJECT_NAME": st.session_state.get('general_business_object_name'),
                "COMPUTE_CLASS": st.session_state.get('general_compute_class'),
                "COMPUTE_CLASS_DEV": st.session_state.get('general_compute_class_dev'),
                "COST_CENTER": st.session_state.get('general_cost_center'),
                "WARNING_DL_GROUP": st.session_state.get('general_warning_dl_group'),
                "SPARK_CONFIGS": st.session_state.get('general_spark_configs'),
                "min_version": st.session_state.get('general_min_version'),
                "max_version": st.session_state.get('general_max_version'),
                "INSERTED_BY": st.session_state.get('general_inserted_by'),
                "UPDATED_BY": st.session_state.get('general_updated_by'),
            }
            l0_tables_data_list = []
            if current_layer == "L0":
                num_tables_to_save = int(st.session_state.get('l0_num_tables_input', 1))
                for i in range(num_tables_to_save):
                    table_data = {
                       # # Each key must match the f-string key used in the form
                        #"l0_id": st.session_state.get(f'l0_id_{i}'),
                        "LOB": st.session_state.get(f"lob_{i}"),
                        "SOURCE": st.session_state.get(f"source_{i}"),
                        "SOURCE_OBJ_SCHEMA": st.session_state.get(f"source_schema_{i}"),
                        "SOURCE_OBJ_NAME": st.session_state.get(f"source_obj_name_{i}"),
                        "INPUT_FILE_FORMAT": st.session_state.get(f"input_file_format_{i}"),
                        "STORAGE_TYPE": st.session_state.get(f"storage_type_{i}"),
                        "CUSTOM_SCHEMA": st.session_state.get(f"custom_schema_{i}"),
                        "DELIMETER": st.session_state.get(f"delimeter_{i}"),
                        "DQ_LOGIC": st.session_state.get(f"dq_logic_{i}"),
                        "CDC_LOGIC": st.session_state.get(f"cdc_logic_{i}"),
                        "TRANSFORM_QUERY": st.session_state.get(f"transform_query_{i}"),
                        "LOAD_TYPE": st.session_state.get(f"load_type_{i}"),
                        "PRESTAG_FLAG": st.session_state.get(f"prestag_flag_{i}"),
                        "PARTITION": st.session_state.get(f"partition_{i}"),
                        "IS_ACTIVE": st.session_state.get(f"status_{i}"),
                        "LS_FLAG": st.session_state.get(f"ls_flag_{i}", 'N'),
                        "LS_DETAIL": st.session_state.get(f"ls_detail_{i}", '')
                    }
                    l0_tables_data_list.append(table_data)       
            pb_data = {}
            if current_layer in ["L1", "L2"]:
                pb_data = {
                    #"pb_id": st.session_state.get('pb_id'),
                    "TARGET_OBJ_TYPE": st.session_state.get('target_obj_type_L1_L2'),
                    "LOB": st.session_state.get('lob_L1_L2'),
                    "SOURCE_PK": st.session_state.get('source_pk_L1_L2'),
                    "TARGET_OBJ_SCHEMA": st.session_state.get('target_obj_schema_L1_L2'),
                    "PRIORITY": st.session_state.get('priority_L1_L2'),
                    "TRANSFORM_QUERY": st.session_state.get('transform_query_L1_L2'),
                    "TARGET_OBJ_NAME": st.session_state.get('target_obj_name_L1_L2'),
                    "GENERIC_SCRIPTS": st.session_state.get('generic_scripts_L1_L2'),
                    "TARGET_PK": st.session_state.get('target_pk_L1_L2'),
                    "LOAD_TYPE": st.session_state.get('load_type_L1_L2'),
                    "PARTITION_METHOD": st.session_state.get('partition_method_L1_L2'),
                    "PARTITION_OR_INDEX": st.session_state.get('partition_or_index_L1_L2'),
                    "CUSTOM_SCRIPT_PARAMS": st.session_state.get('custom_script_params_L1_L2'),
                    "RETENTION_DETAILS": st.session_state.get('retention_details_L1_L2'),
                    "IS_ACTIVE": st.session_state.get('status_L1_L2'),
                }                         
            # l0_tables_data_list = []
            # if current_layer == "L0":
            #     num_tables_to_save = int(st.session_state.get('l0_num_tables_input', 1))
            #     for i in range(num_tables_to_save):
            #         table_data = {
            #             "l0_id": st.session_state.get(f'l0_id_{i}'),
            #             "LOB": st.session_state.get(f"lob_{i}"),
            #             "SOURCE": st.session_state.get(f"source_{i}"),
            #             "SOURCE_OBJ_SCHEMA": st.session_state.get(f"source_schema_{i}"),
            #             "SOURCE_OBJ_NAME": st.session_state.get(f"source_obj_name_{i}"),
            #             "INPUT_FILE_FORMAT": st.session_state.get(f"input_file_format_{i}"),
            #             "STORAGE_TYPE": st.session_state.get(f"storage_type_{i}"),
            #             "CUSTOM_SCHEMA": st.session_state.get(f"custom_schema_{i}"),
            #             "DELIMETER": st.session_state.get(f"delimeter_{i}"),
            #             "DQ_LOGIC": st.session_state.get(f"dq_logic_{i}"),
            #             "CDC_LOGIC": st.session_state.get(f"cdc_logic_{i}"),
            #             "TRANSFORM_QUERY": st.session_state.get(f"transform_query_{i}"),
            #             "LOAD_TYPE": st.session_state.get(f"load_type_{i}"),
            #             "PRESTAG_FLAG": st.session_state.get(f"prestag_flag_{i}"),
            #             "PARTITION": st.session_state.get(f"partition_{i}"),
            #             "IS_ACTIVE": st.session_state.get(f"status_{i}"),
            #             "LS_FLAG": st.session_state.get(f"ls_flag_{i}", 'N'),
            #             "LS_DETAIL": st.session_state.get(f"ls_detail_{i}", '')
            #         }
            #         l0_tables_data_list.append(table_data)
            
            # pb_data = {}
            # if current_layer in ["L1", "L2"]:
            #     pb_data = {
            #         "pb_id": st.session_state.get('pb_id'),
            #         "TARGET_OBJ_TYPE": st.session_state.get('target_obj_type_L1_L2'),
            #         "LOB": st.session_state.get('lob_L1_L2'),
            #         "SOURCE_PK": st.session_state.get('source_pk_L1_L2'),
            #         "TARGET_OBJ_SCHEMA": st.session_state.get('target_obj_schema_L1_L2'),
            #         "PRIORITY": st.session_state.get('priority_L1_L2'),
            #         "TRANSFORM_QUERY": st.session_state.get('transform_query_L1_L2'),
            #         "TARGET_OBJ_NAME": st.session_state.get('target_obj_name_L1_L2'),
            #         "GENERIC_SCRIPTS": st.session_state.get('generic_scripts_L1_L2'),
            #         "TARGET_PK": st.session_state.get('target_pk_L1_L2'),
            #         "LOAD_TYPE": st.session_state.get('load_type_L1_L2'),
            #         "PARTITION_METHOD": st.session_state.get('partition_method_L1_L2'),
            #         "PARTITION_OR_INDEX": st.session_state.get('partition_or_index_L1_L2'),
            #         "CUSTOM_SCRIPT_PARAMS": st.session_state.get('custom_script_params_L1_L2'),
            #         "RETENTION_DETAILS": st.session_state.get('retention_details_L1_L2'),
            #         "IS_ACTIVE": st.session_state.get('status_L1_L2'),
            #     }
            
            is_valid = True
            required_general_fields = ["DATA_FLOW_GROUP_ID", "TRIGGER_TYPE", "ETL_LAYER", "COMPUTE_CLASS", "COMPUTE_CLASS_DEV", "DATA_SME", "PRODUCT_OWNER", "WARNING_THRESHOLD_MINS", "WARNING_DL_GROUP"]
            for field in required_general_fields:
                if not general_data.get(field):
                    st.error(f"Please fill in all required General fields marked with an asterisk (*). Missing field: '{field}'")
                    is_valid = False
                    break
            
            if is_valid and current_layer == "L0":
                if not general_data.get("INGESTION_MODE"):
                    st.error("Please fill in the required field: 'INGESTION_MODE'")
                    is_valid = False
                
                required_l0_fields = ["LOB", "SOURCE", "SOURCE_OBJ_SCHEMA", "SOURCE_OBJ_NAME", "INPUT_FILE_FORMAT", "STORAGE_TYPE", "DQ_LOGIC", "CDC_LOGIC", "TRANSFORM_QUERY", "LOAD_TYPE", "PRESTAG_FLAG", "IS_ACTIVE"]
                for table_data in l0_tables_data_list:
                    for field in required_l0_fields:
                        if not table_data.get(field):
                            st.error(f"Please fill in all required fields for all tables. Missing field: {field}")
                            is_valid = False
                            break
                    if not is_valid: break
            
            if is_valid and current_layer in ["L1", "L2"]:
                required_pb_fields = ["LOB", "TARGET_OBJ_SCHEMA", "PRIORITY", "TARGET_OBJ_TYPE", "TARGET_OBJ_NAME", "TRANSFORM_QUERY", "LOAD_TYPE", "IS_ACTIVE"]
                for field in required_pb_fields:
                    if not pb_data.get(field):
                        st.error(f"Please fill in all required fields for L{current_layer}. Missing field: '{field}'")
                        is_valid = False
                        break
                if st.session_state.get('target_obj_type_L1_L2') == "Table":
                    if not st.session_state.get('retention_details_L1_L2'):
                        st.error("Please fill in the required field: 'Retention details'")
                        is_valid = False
            
            # if is_valid:
            #     try:
            #         if st.session_state.edit_pipeline_id:
            #             database.update_general_info(general_data, st.session_state.edit_pipeline_id)
            #             if current_layer == "L0":
            #                 database.update_l0_details(l0_tables_data_list, st.session_state.edit_pipeline_id)
            #             elif current_layer in ["L1", "L2"]:
            #                 database.update_pb_details(pb_data, st.session_state.edit_pipeline_id)
            #             st.success("Pipeline data updated successfully! ✅")
            
            if is_valid:
                try:
                    if st.session_state.edit_pipeline_id:
                        database.update_general_info(general_data, st.session_state.edit_pipeline_id)
                        if current_layer == "L0":
                            # Ensure the data list is correctly passed
                            database.update_l0_details(l0_tables_data_list, st.session_state.edit_pipeline_id)
                        elif current_layer in ["L1", "L2"]:
                            # Ensure the data dictionary is correctly passed
                            database.update_pb_details(pb_data, st.session_state.edit_pipeline_id)
                        st.success("Pipeline data updated successfully! ✅")
                    else:
                        database.save_general_info(general_data)
                        if current_layer == "L0":
                            database.save_l0_details(l0_tables_data_list, general_data['DATA_FLOW_GROUP_ID'])
                        elif current_layer in ["L1", "L2"]:
                            database.save_pb_details(pb_data, general_data['DATA_FLOW_GROUP_ID'])
                        st.success("Pipeline data saved successfully! ✅")
                except Exception as e:
                    st.error(f"Failed to save data. Please check logs for details. Error: {e}")
                
                st.session_state.form_visible = False
                st.session_state.edit_pipeline_id = None
                st.rerun()
                
    else:
        st.subheader("Quick Actions")
        st.write("Start creating a new pipeline or choose from templates")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.container(border=True, height=120).markdown(f"""### Raw (L0) Layer\nRaw layer...""")
            if st.button("Create Raw Pipeline", key="create_raw", use_container_width=True):
                st.session_state.edit_pipeline_id = None
                st.session_state.current_pipeline_layer = "L0"
                st.session_state.form_visible = True
                st.session_state['l0_tables_data'] = [{}]
                st.session_state['general_data'] = {}
                st.session_state['pb_data'] = {}
                st.rerun()
        with col2:
            st.container(border=True, height=120).markdown(f"""### Curated (L1) Layer\nCurated layer...""")
            if st.button("Create Curated Pipeline", key="create_curated", use_container_width=True):
                st.session_state.edit_pipeline_id = None
                st.session_state.current_pipeline_layer = "L1"
                st.session_state.form_visible = True
                st.session_state['pb_data'] = {}
                st.session_state['general_data'] = {}
                st.session_state['l0_tables_data'] = [{}]
                st.rerun()
        with col3:
            st.container(border=True, height=120).markdown(f"""### Data Product (L2) Layer\nData Product layer...""")
            if st.button("Create Data Product Pipeline", key="create_data_product", use_container_width=True):
                st.session_state.edit_pipeline_id = None
                st.session_state.current_pipeline_layer = "L2"
                st.session_state.form_visible = True
                st.session_state['pb_data'] = {}
                st.session_state['general_data'] = {}
                st.session_state['l0_tables_data'] = [{}]
                st.rerun()
        st.subheader("Recent Activity")
        st.write("Recently modified pipelines across all layers")
        pipelines = database.get_all_pipelines()
        if not pipelines:
            st.info("No pipelines found. Create one to get started! 🚀")
        else:
            for i, p in enumerate(pipelines):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"**{p.get('DATA_FLOW_GROUP_ID', 'N/A')}**")
                    st.markdown(f"*{p.get('BUSINESS_UNIT', 'N/A')}* • Updated {p.get('UPDATED_TS', 'N/A')}")
                with col2:
                    if st.button("Edit", key=f"edit_{p.get('DATA_FLOW_GROUP_ID')}_{i}"):
                        st.session_state.edit_pipeline_id = p.get('DATA_FLOW_GROUP_ID')
                        st.session_state.current_pipeline_layer = p['ETL_LAYER']
                        st.session_state.form_visible = True
                        st.rerun()
                st.markdown("---")