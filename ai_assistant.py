import streamlit as st
import google.generativeai as genai
import json
import re
import database as db
import sqlite3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure the database is initialized
db.init_db()

# Gemini API key configuration - Load from environment variable
API_KEY = os.getenv("GOOGLE_API_KEY")

if not API_KEY:
    st.error("API Key not found. Please set the GOOGLE_API_KEY environment variable in your .env file.")
    st.stop()

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel(model_name="gemini-2.5-flash")


# System Prompts for different tables and operations
SYSTEM_PROMPTS = {
    "header": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to extract specific field values from a user's plain English input.

    You MUST follow these rules:
    1.  **Extract Data**: Parse the user's input to extract values for the specified fields.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should only contain the keys for the fields you were able to extract from the user's message. Do not include any other text or formatting.
        **Example:** For the input "create pipeline with ID my_pipeline, Business Unit is Finace, ETL Layer is L0", the correct output is `{"DATA_FLOW_GROUP_ID": "my_pipeline", "BUSINESS_UNIT": "Finace", "ETL_LAYER": "L0"}`.
    3.  **No Extraneous Keys**: If a value for a key is not explicitly mentioned or inferred from the user's input, DO NOT include that key in the JSON response.
    4.  **Schema and Rules**: Use the following database schema and rules to guide your extraction and validation.
        -   **data_flow_control_header**:
            -   DATA_FLOW_GROUP_ID (STRING, required)
            -   BUSINESS_UNIT (STRING, required)
            -   BUSINESS_OBJECT_NAME (STRING, required)
            -   TRIGGER_TYPE (STRING, required): Allowed values: DLT, JOB. Dependency: Must be DLT if ETL_LAYER is L0.
            -   ETL_LAYER (STRING, required): Allowed values: L0, L1, L2.
            -   COMPUTE_CLASS (STRING, required)
            -   COMPUTE_CLASS_DEV (STRING, required)
            -   DATA_SME (STRING, required)
            -   PRODUCT_OWNER (STRING, required)
            -   INGESTION_BUCKET (STRING, required): 
            -   WARNING_THRESHOLD_MINS (INT, required)
            -   WARNING_DL_GROUP (STRING, required)
            -   IS_ACTIVE (STRING, required): Allowed values: Y, N. Default: Y.
            -   INGESTION_MODE (STRING, required for L0)
            -   SPARK_CONFIGS (STRING)
            -   COST_CENTER (STRING)
            -   min_version (DECIMAL)
            -   max_version (DECIMAL)
    """,
    "l0": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to extract specific field values from a user's plain English input for the L0 layer.

    You MUST follow these rules:
    1.  **Extract Data**: Parse the user's input to extract values for the specified fields.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should only contain the keys for the fields you were able to extract from the user's message. Do not include any other text or formatting.
    3.  **No Extraneous Keys**: If a value for a key is not explicitly mentioned or inferred from the user's input, DO NOT include that key in the JSON response.
    4.  **Schema and Rules**: Use the following database schema and rules to guide your extraction and validation.
        -   **data_flow_l0_detail**:
            -   SOURCE (STRING, required)
            -   SOURCE_OBJ_SCHEMA (STRING, required)
            -   SOURCE_OBJ_NAME (STRING, required)
            -   LOB (STRING, required)
            -   INPUT_FILE_FORMAT (STRING, required)
            -   STORAGE_TYPE (STRING, required): Allowed values: C1, C2, C3, C4.
            -   DQ_LOGIC (STRING, required)
            -   CDC_LOGIC (STRING, required)
            -   TRANSFORM_QUERY (STRING, required)
            -   LOAD_TYPE (STRING, required): Allowed values: FULL, DELTA, SCD, PySpark.
            -   PRESTAG_FLAG (STRING, required): Allowed values: Y, N.
            -   CUSTOM_SCHEMA (STRING)
            -   DELIMETER (STRING)
            -   PARTITION (STRING)
            -   IS_ACTIVE (STRING, required): Allowed values: Y, N. Default: Y.
    """,
    "l1_l2": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to extract specific field values from a user's plain English input for the L1 or L2 layer.

    You MUST follow these rules:
    1.  **Extract Data**: Parse the user's input to extract values for the specified fields.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should only contain the keys for the fields you were able to extract from the user's message. Do not include any other text or formatting.
    3.  **No Extraneous Keys**: If a value for a key is not explicitly mentioned or inferred from the user's input, DO NOT include that key in the JSON response.
    4.  **Schema and Rules**: Use the following database schema and rules to guide your extraction and validation.
        -   **data_flow_pb_detail**:
            -   LOB (STRING, required)
            -   TARGET_OBJ_SCHEMA (STRING, required)
            -   TARGET_OBJ_NAME (STRING, required)
            -   PRIORITY (INT, required)
            -   TARGET_OBJ_TYPE (STRING, required): Allowed values: Table, MV. Dependency: If Table, TRIGGER_TYPE must be JOB. If MV, TRIGGER_TYPE must be DLT.
            -   TRANSFORM_QUERY (STRING, required)
            -   LOAD_TYPE (STRING, required): Allowed values: FULL, DELTA, SCD. Dependency: If SCD, CUSTOM_SCRIPT_PARAMS becomes mandatory.
            -   PARTITION_METHOD (STRING, optional): Allowed values: Partition, Liquid cluster.
            -   CUSTOM_SCRIPT_PARAMS (STRING, optional)
            -   GENERIC_SCRIPTS (STRING)
            -   SOURCE_PK (STRING)
            -   TARGET_PK (STRING)
            -   PARTITION_OR_INDEX (STRING)
            -   RETENTION_DETAILS (STRING)
            -   IS_ACTIVE (STRING, required): Allowed values: Y, N. Default: Y.
    """,
    "show_table": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to identify a user's request to 'show' a pipeline's details.

    You MUST follow these rules:
    1.  **Identify Action**: Look for keywords like 'show table', 'display all pipelines', 'list pipelines'.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should have a key `action` with the value `show_all_pipelines`.
    3.  **No Other Keys**: Do not include any other keys in the JSON response.
    4.  **No Other Text**: Do not include any other text or conversational phrases.
    """,
    "show_details": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to identify a user's request to 'show' a pipeline's specific details.

    You MUST follow these rules:
    1.  **Identify Action**: Look for keywords like 'show details', 'view', 'look up', 'get details'.
    2.  **Extract Data**: Extract the **DATA_FLOW_GROUP_ID** from the user's input.
    3.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should have a key `action` with the value `show_details` and a key `DATA_FLOW_GROUP_ID` with the extracted value.
    4.  **No Other Keys**: Do not include any other keys in the JSON response.
    5.  **No Other Text**: Do not include any other text or conversational phrases.
    """
}

DEFAULT_VALUES = {
    "header": {
        # Required fields
        "BUSINESS_OBJECT_NAME": "default_object",
        "TRIGGER_TYPE": "DLT",  # Default to DLT, common for new pipelines
        "COMPUTE_CLASS": "L_C5", # Assuming 'Medium' is a default option
        "COMPUTE_CLASS_DEV": "L_C5", # Assuming 'Small' is a default dev option
        "DATA_SME": "sme@example.com",
        "PRODUCT_OWNER": "owner@example.com",
        "INGESTION_BUCKET": "default-ingestion-bucket",
        "WARNING_THRESHOLD_MINS": 60, # Default warning after 60 minutes
        "WARNING_DL_GROUP": "support-dl@example.com",
        "IS_ACTIVE": "Y", 
        # L0-specific required field
        "INGESTION_MODE": "EXTL_FULL",
        # Optional fields
        "SPARK_CONFIGS": "default.spark.config=true",
        "COST_CENTER": "CC1000",
        "min_version": 1.0,
        "max_version": 9999.0
    },
    "l0": {
        # Required fields
        "SOURCE_OBJ_SCHEMA": "default_source_schema",
        "SOURCE_OBJ_NAME": "default_source_table",
        "LOB": "Technology",
        "INPUT_FILE_FORMAT": "parquet",
        "STORAGE_TYPE": "C1",
        "DQ_LOGIC": "default_dq_rule",
        "CDC_LOGIC": "NO_CDC",
        "TRANSFORM_QUERY": "SELECT * FROM source_table",
        "LOAD_TYPE": "FULL",
        "PRESTAG_FLAG": "N",
        "IS_ACTIVE": "Y",
        # Optional fields
        "CUSTOM_SCHEMA": "col1:string,col2:int",
        "DELIMETER": "|",
        "PARTITION": "load_date"
    },
    "pb": {
        # Required fields (L1/L2 detail)
        "LOB": "Finance",
        "TARGET_OBJ_SCHEMA": "default_target_schema",
        "TARGET_OBJ_NAME": "default_target_table",
        "PRIORITY": 1,
        "TARGET_OBJ_TYPE": "Table",
        "TRANSFORM_QUERY": "SELECT * FROM l0_data",
        "LOAD_TYPE": "DELTA",
        "IS_ACTIVE": "Y",
        # Optional fields
        "PARTITION_METHOD": "Partition",
        "CUSTOM_SCRIPT_PARAMS": "param1=value",
        "GENERIC_SCRIPTS": "audit_log_script",
        "SOURCE_PK": "source_key",
        "TARGET_PK": "target_key",
        "PARTITION_OR_INDEX": "partition_key",
        "RETENTION_DETAILS": "365 days"
    }
}
compute_class_options = db.get_compute_classes(dev_allowed=False)
dev_compute_class_options = db.get_compute_classes(dev_allowed=True)

# The VALID_OPTIONS dictionary is updated to use the fetched lists
VALID_OPTIONS = {
    "IS_ACTIVE": ["Y", "N"],
    "TRIGGER_TYPE": ["DLT", "JOB"],
    "ETL_LAYER": ["L0", "L1", "L2"],
    "COMPUTE_CLASS": compute_class_options,
    "COMPUTE_CLASS_DEV": dev_compute_class_options,
    "INGESTION_MODE": ["EXTL_FULL", "EXTL_INC", "DATASPHERE_INGEST", "API_INGEST", "DB_INGEST"],
    "STORAGE_TYPE": ["C1", "C2", "C3", "C4"],
    "INPUT_FILE_FORMAT": ["parquet", "csv", "tsv", "json", "xml"],
    "LOAD_TYPE": ["FULL", "DELTA", "SCD", "PySpark"],
    "PRESTAG_FLAG": ["Y", "N"],
    "TARGET_OBJ_TYPE": ["MV", "Table", "View"],
    #"PRIORITY": ["Low", "Medium", "High", "Critical"],
    "PARTITION_METHOD": ["Partition", "Liquid cluster"],
}


REQUIRED_FIELDS_HEADER = [
    "DATA_FLOW_GROUP_ID", "BUSINESS_UNIT", "BUSINESS_OBJECT_NAME", "TRIGGER_TYPE",
    "ETL_LAYER", "COMPUTE_CLASS", "COMPUTE_CLASS_DEV", "DATA_SME", "PRODUCT_OWNER",
     "WARNING_THRESHOLD_MINS", "WARNING_DL_GROUP", "IS_ACTIVE"
]

REQUIRED_FIELDS_L0 = [
    "SOURCE", "SOURCE_OBJ_SCHEMA", "SOURCE_OBJ_NAME", "LOB", "INPUT_FILE_FORMAT",
    "STORAGE_TYPE", "DQ_LOGIC", "CDC_LOGIC", "TRANSFORM_QUERY", "LOAD_TYPE",
    "PRESTAG_FLAG", "IS_ACTIVE"
]

REQUIRED_FIELDS_PB = [
    "LOB", "TARGET_OBJ_SCHEMA", "TARGET_OBJ_NAME", "PRIORITY", "TARGET_OBJ_TYPE",
    "TRANSFORM_QUERY", "LOAD_TYPE", "IS_ACTIVE"
]

# All possible fields for each table, including optional ones
ALL_FIELDS_HEADER = REQUIRED_FIELDS_HEADER + ["INGESTION_MODE", "INGESTION_BUCKET", "SPARK_CONFIGS", "COST_CENTER", "min_version", "max_version"]
ALL_FIELDS_L0 = REQUIRED_FIELDS_L0 + ["CUSTOM_SCHEMA", "DELIMETER", "PARTITION"]
ALL_FIELDS_PB = REQUIRED_FIELDS_PB + ["PARTITION_METHOD", "CUSTOM_SCRIPT_PARAMS", "GENERIC_SCRIPTS", "SOURCE_PK", "TARGET_PK", "PARTITION_OR_INDEX", "RETENTION_DETAILS"]

# Map for L0 table numbers in modify mode
L0_TABLE_MAP = {
    'table1': 0, 't1': 0, 'table 1': 0, 't 1': 0, 'table_1': 0, 't_1': 0,
    'table2': 1, 't2': 1, 'table 2': 1, 't 2': 1, 'table_2': 1, 't_2': 1,
    'table3': 2, 't3': 2, 'table 3': 2, 't 3': 2, 'table_3': 2, 't_3': 2,
    'table4': 3, 't4': 3, 'table 4': 3, 't 4': 3, 'table_4': 3, 't_4': 3,
    'table5': 4, 't5': 4, 'table 5': 4, 't 5': 4,' table_5': 4, 't_5': 4
}

# Mapping for user-friendly names
FIELD_MAPPING = {
    "DATA_FLOW_GROUP_ID": "data flow name",
    "BUSINESS_UNIT": "business unit",
    "BUSINESS_OBJECT_NAME": "business object name",
    "TRIGGER_TYPE": "trigger type",
    "ETL_LAYER": "ETL layer",
    "COMPUTE_CLASS": "compute class",
    "COMPUTE_CLASS_DEV": "dev compute class",
    "DATA_SME": "data SME",
    "PRODUCT_OWNER": "product owner",
    "INGESTION_BUCKET": "ingestion bucket",
    "WARNING_THRESHOLD_MINS": "warning threshold (mins)",
    "WARNING_DL_GROUP": "warning DL group",
    "IS_ACTIVE": "is active",
    "INGESTION_MODE": "ingestion mode",
    "SOURCE": "source",
    "SOURCE_OBJ_SCHEMA": "source schema",
    "SOURCE_OBJ_NAME": "source object name",
    "LOB": "LOB",
    "INPUT_FILE_FORMAT": "input file format",
    "STORAGE_TYPE": "storage type",
    "DQ_LOGIC": "DQ logic",
    "CDC_LOGIC": "CDC logic",
    "TRANSFORM_QUERY": "transform query",
    "LOAD_TYPE": "load type",
    "PRESTAG_FLAG": "pre-stage flag",
    "TARGET_OBJ_SCHEMA": "target schema",
    "TARGET_OBJ_NAME": "target object name",
    "PRIORITY": "priority",
    "TARGET_OBJ_TYPE": "target object type",
    "PARTITION_METHOD": "partition method",
    "CUSTOM_SCRIPT_PARAMS": "custom script parameters",
    "SOURCE_PK": "source primary key",
    "TARGET_PK": "target primary key",
    "CUSTOM_SCHEMA": "custom schema",
    "RETENTION_DETAILS": "retention details"
}

def get_required_fields(table_type, etl_layer=None):
    if table_type == "header":
        required = REQUIRED_FIELDS_HEADER.copy()
        if etl_layer and etl_layer.upper() == "L0":
            required.append("INGESTION_MODE")
        return required
    elif table_type == "l0":
        return REQUIRED_FIELDS_L0
    elif table_type == "pb":
        return REQUIRED_FIELDS_PB
    return []

def get_all_fields(table_type):
    if table_type == "header":
        return ALL_FIELDS_HEADER
    elif table_type == "l0":
        return ALL_FIELDS_L0
    elif table_type == "pb":
        return ALL_FIELDS_PB
    return []

# Function to add asterisk to important fields
def get_json_with_asterisks(data, table_type):
    important_fields_map = {
        "header": REQUIRED_FIELDS_HEADER,
        "l0": REQUIRED_FIELDS_L0,
        "pb": REQUIRED_FIELDS_PB
    }
    important_fields = important_fields_map.get(table_type, [])
    
    formatted_data = {}
    for key, value in data.items():
        if key in important_fields:
            formatted_data[f"*{key}*"] = value
        else:
            formatted_data[key] = value
    return formatted_data

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)


def validate_data(data, table_type, trigger_type=None):
    missing_fields = []
    invalid_values = []
    
    if table_type == "header":
        required_fields = get_required_fields(table_type, data.get('ETL_LAYER'))
        for field in required_fields:
            if field not in data or data[field] is None or data[field] == "":
                missing_fields.append(FIELD_MAPPING.get(field, field))
        
        # if "PRODUCT_OWNER" in data and data["PRODUCT_OWNER"] is not None and not is_valid_email(data["PRODUCT_OWNER"]):
        #     invalid_values.append(f"PRODUCT_OWNER must be a valid email address.")
        
        etl_layer = (data.get('ETL_LAYER') or '').upper()
        trigger_type = (data.get('TRIGGER_TYPE') or '').upper()
        
        if etl_layer == "L0" and trigger_type != "DLT" and "TRIGGER_TYPE" in data:
             invalid_values.append(f"Friendly message: For an L0 pipeline, the trigger type must always be DLT. Please update the TRIGGER_TYPE field.")

        for key in VALID_OPTIONS:
            if key in data and data[key] is not None and data[key].upper() not in [o.upper() for o in VALID_OPTIONS[key]]:
                invalid_values.append(f"Oops! The value for **{FIELD_MAPPING.get(key, key)}** is not allowed. Only allowed options are: `{', '.join(VALID_OPTIONS[key])}`.")

            
    elif table_type == "l0":
        required_fields = get_required_fields(table_type)
        for field in required_fields:
            if field not in data or data[field] is None or data[field] == "":
                missing_fields.append(FIELD_MAPPING.get(field, field))
        
        for key in VALID_OPTIONS:
            if key in data and data[key] is not None and data[key].upper() not in [o.upper() for o in VALID_OPTIONS[key]]:
                invalid_values.append(f"Oops! The value for **{FIELD_MAPPING.get(key, key)}** is not allowed. Only allowed options are: `{', '.join(VALID_OPTIONS[key])}`.")


    elif table_type == "pb":
        required_fields = get_required_fields(table_type)
        for field in required_fields:
            if field not in data or data[field] is None or data[field] == "":
                missing_fields.append(FIELD_MAPPING.get(field, field))
        
        if "LOAD_TYPE" in data and data["LOAD_TYPE"].upper() == "SCD" and ("CUSTOM_SCRIPT_PARAMS" not in data or data["CUSTOM_SCRIPT_PARAMS"] is None or data["CUSTOM_SCRIPT_PARAMS"] == ""):
            missing_fields.append(FIELD_MAPPING.get("CUSTOM_SCRIPT_PARAMS", "CUSTOM_SCRIPT_PARAMS"))
        
        target_obj_type = (data.get("TARGET_OBJ_TYPE") or '').upper()
        if target_obj_type == "TABLE" and trigger_type and (trigger_type.upper() or '') != "JOB":
            invalid_values.append(f"For TARGET_OBJ_TYPE 'Table', the TRIGGER_TYPE must be 'JOB'.")
        if target_obj_type == "MV" and trigger_type and (trigger_type.upper() or '') != "DLT":
            invalid_values.append(f"For TARGET_OBJ_TYPE 'MV', the TRIGGER_TYPE must be 'DLT'.")
            
        for key in VALID_OPTIONS:
            if key in data and data[key] is not None and data[key].upper() not in [o.upper() for o in VALID_OPTIONS[key]]:
                invalid_values.append(f"Oops! The value for **{FIELD_MAPPING.get(key, key)}** is not allowed. Only allowed options are: `{', '.join(VALID_OPTIONS[key])}`.")
                
    return missing_fields, invalid_values

def get_pipeline_details(data_flow_group_id):
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    header_data = cursor.fetchone()
    
    if not header_data:
        conn.close()
        return None, None
        
    header_columns = [col[0] for col in cursor.description]
    header_dict = dict(zip(header_columns, header_data))
    
    detail_data = None
    if header_dict['ETL_LAYER'].upper() == 'L0':
        cursor.execute("SELECT * FROM data_flow_l0_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        detail_rows = cursor.fetchall()
        if detail_rows:
            detail_columns = [col[0] for col in cursor.description]
            detail_data = [dict(zip(detail_columns, row)) for row in detail_rows]
    else:
        cursor.execute("SELECT * FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        detail_row = cursor.fetchone()
        if detail_row:
            detail_columns = [col[0] for col in cursor.description]
            detail_data = dict(zip(detail_columns, detail_row))
            
    conn.close()
    return header_dict, detail_data

def get_all_pipelines_summary():
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DATA_FLOW_GROUP_ID, BUSINESS_UNIT, ETL_LAYER, PRODUCT_OWNER FROM data_flow_control_header")
    pipelines = cursor.fetchall()
    conn.close()
    return pipelines



def check_and_transition_stage():
    current_stage = st.session_state.conversation_stage
    header_data = st.session_state.pipeline_data.get('header', {})
    detail_data = st.session_state.pipeline_data.get('detail', {})
    
    defaulted_fields = []
    
    # 1. Check for the default value trigger in the last user message
    last_user_prompt = ""
    # Check if messages list is not empty and the last message is from the user
    if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
        last_user_prompt = st.session_state.messages[-1]["content"].lower()

    apply_defaults = "default value" in last_user_prompt
    
    # --- Context Determination and Data Cleaning (Edge Cases) ---

    # Initialize context variables. These will be set correctly for the current stage.
    required_fields = []
    default_map = {}
    data_to_check = None
    table_type = None

    # Handle L1/L2 data cleaning before validation/defaults
    if current_stage in ["detail_pb_in_progress", "header_in_progress"]:
        etl_layer = (header_data.get('ETL_LAYER') or '').upper()
        
        # Edge Case 2: Ingestion fields for L1/L2
        if etl_layer in ["L1", "L2"]:
            ing_fields = ["INGESTION_MODE", "INGESTION_BUCKET"]
            cleared_fields = []
            for field in ing_fields:
                if header_data.pop(field, None) is not None:
                    cleared_fields.append(FIELD_MAPPING.get(field))
            if cleared_fields:
                st.session_state.messages.append({"role": "assistant", "content": f"Hi there! The fields {', '.join(cleared_fields)} are only needed for L0 pipelines. I've removed them for you."})
                
        # Edge Case 3: Target object fields for L1/L2
        if etl_layer in ["L1", "L2"] and isinstance(detail_data, dict):
            target_obj_type = (detail_data.get('TARGET_OBJ_TYPE') or '').upper()
            if target_obj_type != "TABLE":
                pk_fields = ["SOURCE_PK", "TARGET_PK", "CUSTOM_SCHEMA", "RETENTION_DETAILS"]
                cleared_fields = []
                for field in pk_fields:
                    if detail_data.pop(field, None) is not None:
                        cleared_fields.append(FIELD_MAPPING.get(field))
                if cleared_fields:
                    st.session_state.messages.append({"role": "assistant", "content": f"Hey! Since the target object type is not a 'Table', the fields {', '.join(cleared_fields)} are not needed. I've cleared them out."})

    # --- DEFAULT VALUE APPLICATION AND VALIDATION PRE-CHECK ---

    if current_stage == "header_in_progress":
        table_type = "header"
        required_fields = get_required_fields(table_type, header_data.get('ETL_LAYER'))
        default_map = DEFAULT_VALUES.get(table_type, {})
        data_to_check = header_data
    
    elif current_stage == "detail_l0_in_progress" and isinstance(detail_data, list):
        current_l0_index = st.session_state.get('current_l0_table_index', 0)
        # Safety check for index out of bounds (can happen after reruns)
        if current_l0_index >= len(detail_data):
            # This should ideally not happen, but if it does, exit validation gracefully
            return 
            
        table_type = "l0"
        required_fields = get_required_fields(table_type)
        default_map = DEFAULT_VALUES.get(table_type, {})
        data_to_check = detail_data[current_l0_index]
            
    elif current_stage == "detail_pb_in_progress": 
        # If the stage is PB, we assume st.session_state.pipeline_data['detail'] is a dict (from fix)
        # We reference it directly here.
        if isinstance(detail_data, dict):
            table_type = "pb"
            required_fields = get_required_fields(table_type)
            default_map = DEFAULT_VALUES.get(table_type, {})
            data_to_check = detail_data
        else:
            # Fallback if the state wasn't initialized as a dict (e.g., still {})
            # This prevents the subsequent validation/defaulting blocks from running incorrectly.
            return
    # 1. Identify missing required fields for the current table/stage
    missing_fields_before_default = []
    for field in required_fields:
        if data_to_check.get(field) in [None, ""]:
            missing_fields_before_default.append(field)

    # 2. Apply default values if the trigger phrase was used AND data_to_check is valid
    if apply_defaults and data_to_check is not None:
        for field in missing_fields_before_default:
            if field in default_map:
                # Apply the default value
                data_to_check[field] = default_map[field]
                defaulted_fields.append(FIELD_MAPPING.get(field, field))
                
        if defaulted_fields:
            defaulted_str = ", ".join(defaulted_fields)
            st.session_state.messages.append({"role": "assistant", "content": f"I've applied default values for the following fields: **{defaulted_str}**."})
    
    # 3. Final Validation with the (potentially) updated data
    # We must ensure table_type is set before calling validate_data
    if table_type:
        missing_fields, invalid_values = validate_data(data_to_check, table_type, header_data.get('TRIGGER_TYPE'))
    else:
        # Should not happen, but as a safeguard
        return 


    # --- Error Reporting and Data Cleaning on Invalid Values ---
    if invalid_values:
        for msg in invalid_values:
            # Find the friendly field name in the error message
            match = re.search(r'\*\*(.*?)\*\*', msg)
            if match:
                friendly_field_name = match.group(1).lower().replace(" ", "_")
                # Find the corresponding database field name
                db_field = next((key for key, value in FIELD_MAPPING.items() if value.lower().replace(" ", "_") == friendly_field_name), None)
                if db_field:
                    # Clear the invalid value based on the current stage
                    if current_stage == "header_in_progress":
                        st.session_state.pipeline_data['header'][db_field] = None
                    elif current_stage == "detail_l0_in_progress":
                        # Note: data_to_check is a reference to a list item, so using data_to_check[db_field] = None also works
                        st.session_state.pipeline_data['detail'][st.session_state.current_l0_table_index][db_field] = None
                    elif current_stage == "detail_pb_in_progress":
                        st.session_state.pipeline_data['detail'][db_field] = None
            
            st.session_state.messages.append({"role": "assistant", "content": msg})

    if missing_fields:
        missing_str = ", ".join(missing_fields)
        st.session_state.messages.append({"role": "assistant", "content": f"Got it, some fields are missing: **{missing_str}**. Please provide these values."})

    # --- Stage Transition Logic ---
    if not missing_fields and not invalid_values:
        if current_stage == "header_in_progress":
            st.session_state.messages.append({"role": "assistant", "content": "Header fields are complete."})
            etl_layer = (header_data.get('ETL_LAYER') or '').upper()
            if etl_layer == 'L0':
                st.session_state.conversation_stage = "l0_num_tables_in_progress"
                st.session_state.messages.append({"role": "assistant", "content": "How many L0 tables does this pipeline have? (Max 5)"})
            elif etl_layer in ['L1', 'L2']:
                st.session_state.pipeline_data['detail'] = {} 
                st.session_state.conversation_stage = "detail_pb_in_progress"
                st.session_state.messages.append({"role": "assistant", "content": "Please provide details for `data_flow_pb_detail`."})
        
        elif current_stage == "detail_l0_in_progress" and isinstance(detail_data, list):
            st.session_state.current_l0_table_index += 1
            if st.session_state.current_l0_table_index < len(detail_data):
                st.session_state.messages.append({"role": "assistant", "content": f"Details for table {st.session_state.current_l0_table_index} are complete. Please provide details for table {st.session_state.current_l0_table_index + 1}."})
            else:
                st.session_state.conversation_stage = "completed"
                st.session_state.messages.append({"role": "assistant", "content": "All required fields are completed. You can now submit the data."})

        elif current_stage == "detail_pb_in_progress" and isinstance(detail_data, dict):
            st.session_state.conversation_stage = "completed"
            st.session_state.messages.append({"role": "assistant", "content": "All required fields are completed. You can now submit the data."})

    st.rerun()

def show():
    # Main page layout
    if "pipeline_data" not in st.session_state:
        st.session_state.pipeline_data = {"header": {}, "detail": {}}
    if "conversation_stage" not in st.session_state:
        st.session_state.conversation_stage = "initial"
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if 'current_l0_table_index' not in st.session_state:
        st.session_state.current_l0_table_index = 0
    if "last_prompt_is_get" not in st.session_state:
        st.session_state.last_prompt_is_get = False
    
    # Initialize all required fields in session state
    for field in ALL_FIELDS_HEADER:
        if field not in st.session_state.pipeline_data['header']:
            st.session_state.pipeline_data['header'][field] = None
    
    if 'detail' in st.session_state.pipeline_data and isinstance(st.session_state.pipeline_data['detail'], list):
        for i in range(len(st.session_state.pipeline_data['detail'])):
            for field in ALL_FIELDS_L0:
                if field not in st.session_state.pipeline_data['detail'][i]:
                    st.session_state.pipeline_data['detail'][i][field] = None
    elif 'detail' in st.session_state.pipeline_data and isinstance(st.session_state.pipeline_data['detail'], dict):
        for field in ALL_FIELDS_PB:
            if field not in st.session_state.pipeline_data['detail']:
                st.session_state.pipeline_data['detail'][field] = None
    
    # Sidebar for displaying collected fields and submit button
    with st.sidebar:
        st.header("Collected Fields")
        is_data_complete = False
        
        # Display header data and check for completion
        st.subheader("`data_flow_control_header`")
        header_data = st.session_state.pipeline_data.get("header", {})
        
        # Dynamic display for header fields (Edge Case 2)
        etl_layer = (header_data.get('ETL_LAYER') or '').upper()
        display_fields_header = ALL_FIELDS_HEADER.copy()
        if etl_layer in ["L1", "L2"]:
            display_fields_header.remove("INGESTION_MODE")
            display_fields_header.remove("INGESTION_BUCKET")
            
        header_display_data = {key: header_data.get(key, None) for key in display_fields_header}
        st.json(get_json_with_asterisks(header_display_data, "header"))
        
        header_required_fields = REQUIRED_FIELDS_HEADER.copy()
        if etl_layer == "L0":
            header_required_fields.append("INGESTION_MODE")
        is_header_complete = all(header_data.get(field) is not None for field in header_required_fields)

        # Display detail layer data and check for completion based on ETL_LAYER
        is_detail_complete = False
        detail_data = st.session_state.pipeline_data.get("detail", {})

        if etl_layer == "L0":
            if isinstance(detail_data, list):
                st.subheader(f"`data_flow_l0_detail` (No. of Tables: {len(detail_data)})")
                is_all_l0_complete = True
                for i, table_data in enumerate(detail_data):
                    st.write(f"**Table {i+1}**:")
                    l0_display_data = {key: table_data.get(key, None) for key in ALL_FIELDS_L0}
                    st.json(get_json_with_asterisks(l0_display_data, "l0"))
                    is_current_l0_complete = all(table_data.get(field) is not None for field in REQUIRED_FIELDS_L0)
                    if not is_current_l0_complete:
                        is_all_l0_complete = False
                is_detail_complete = is_all_l0_complete
            else:
                st.subheader("`data_flow_l0_detail`")
                st.json({key: None for key in ALL_FIELDS_L0})

        elif etl_layer in ["L1", "L2"]:
            st.subheader("`data_flow_pb_detail`")
            if isinstance(detail_data, dict):
                # Dynamic display for detail fields (Edge Case 3)
                display_fields_pb = ALL_FIELDS_PB.copy()
                if (detail_data.get('TARGET_OBJ_TYPE') or '').upper() != "TABLE":
                    for field in ["SOURCE_PK", "TARGET_PK", "CUSTOM_SCHEMA", "RETENTION_DETAILS"]:
                        if field in display_fields_pb:
                            display_fields_pb.remove(field)
                            
                detail_display_data = {key: detail_data.get(key, None) for key in display_fields_pb}
                st.json(get_json_with_asterisks(detail_display_data, "pb"))
                is_detail_complete = all(detail_data.get(field) is not None for field in REQUIRED_FIELDS_PB)
            else:
                st.json({key: None for key in ALL_FIELDS_PB})
        
        # Check if both header and detail are complete
        if etl_layer in ["L0", "L1", "L2"]:
            is_data_complete = is_header_complete and is_detail_complete
        else:
            is_data_complete = is_header_complete

        if st.button("Submit", disabled=not is_data_complete, use_container_width=True):
            if is_data_complete:
                st.session_state.current_view = 'add_edit'
                st.session_state.form_visible = True
                
                if etl_layer == "L0":
                    prefill_data = {
                        "header": st.session_state.pipeline_data.get('header', {}),
                        "detail": st.session_state.pipeline_data.get('detail', [])
                    }
                elif etl_layer in ["L1", "L2"]:
                    prefill_data = {
                        "header": st.session_state.pipeline_data.get('header', {}),
                        "detail": [st.session_state.pipeline_data.get('detail', {})]
                    }
                else:
                    prefill_data = {
                        "header": st.session_state.pipeline_data.get('header', {}),
                        "detail": []
                    }
                st.session_state.ai_collected_data = prefill_data
                
                st.session_state.edit_pipeline_id = None
                st.session_state.messages = []
                st.session_state.pipeline_data = {"header": {}, "detail": {}}
                st.session_state.conversation_stage = "initial"
                st.rerun()
            else:
                st.warning("Please complete all required fields before submitting.")

    # Main content area for the conversation
    st.markdown("## Data Pipeline Assistant")
    st.markdown("---") 

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if not st.session_state.messages:
        st.session_state.messages.append({"role": "assistant", "content": "Hello there! I'm an AI assistant to help you create a new data pipeline or view an existing one."})
        st.session_state.conversation_stage = "in_progress"
        st.rerun()

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
        
    if prompt := st.chat_input("Enter pipeline details..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        prompt_to_parse = prompt
        
        # Check for commands at any stage
        if "show table" in prompt.lower() or "display all pipelines" in prompt.lower() or "list pipelines" in prompt.lower():
            st.session_state.last_prompt_is_get = True
            pipelines = get_all_pipelines_summary()
            if pipelines:
                summary_markdown = "Here is a list of all existing pipelines:\n\n"
                for p in pipelines:
                    summary_markdown += f"- **{p[0]}**: BU: `{p[1]}`, Layer: `{p[2]}`, Owner: `{p[3]}`\n"
                summary_markdown += "\nTo see more details, please specify a `DATA_FLOW_GROUP_ID` (e.g., 'show details for retail_sales_dwh')."
                st.session_state.messages.append({"role": "assistant", "content": summary_markdown})
            else:
                st.session_state.messages.append({"role": "assistant", "content": "There are no pipelines in the database yet."})
            st.session_state.conversation_stage = "initial"
            st.rerun()
        
        elif any(keyword in prompt.lower() for keyword in ["show details", "view", "look up", "get details"]):
            st.session_state.last_prompt_is_get = True
            full_prompt = f"{SYSTEM_PROMPTS['show_details']}\nUser input: {prompt_to_parse}"
            response = model.generate_content(full_prompt)
            
            try:
                response_text = response.text.replace("```json", "").replace("```", "").strip()
                extracted_data = json.loads(response_text)
                
                if extracted_data.get("action") == "show_details" and "DATA_FLOW_GROUP_ID" in extracted_data:
                    data_flow_group_id = extracted_data["DATA_FLOW_GROUP_ID"]
                    header_data, detail_data = get_pipeline_details(data_flow_group_id)
                    
                    if header_data:
                        st.session_state.pipeline_data['header'] = header_data
                        st.session_state.pipeline_data['detail'] = detail_data
                        st.session_state.conversation_stage = "view_only"
                        st.session_state.messages.append({"role": "assistant", "content": "View data in side panel"})
                    else:
                        st.session_state.messages.append({"role": "assistant", "content": f"I could not find a pipeline with the ID `{data_flow_group_id}`. Please check the ID and try again."})
                else:
                    st.session_state.messages.append({"role": "assistant", "content": "I'm sorry, I couldn't understand that request. Please try to phrase it clearly, for example: 'show me the details for pipeline [ID]'."})
                    
            except (json.JSONDecodeError, ValueError) as e:
                st.session_state.messages.append({"role": "assistant", "content": f"An unexpected error occurred: {e}. Please try again."})
            st.session_state.conversation_stage = "initial"
            st.rerun()

        elif prompt.lower().strip().startswith("create"):
            st.session_state.pipeline_data = {"header": {}, "detail": {}}
            st.session_state.conversation_stage = "header_in_progress"
            prompt_to_parse = prompt[len("create"):].strip()
            
            if not prompt_to_parse:
                st.session_state.messages.append({"role": "assistant", "content": "Okay, let's create a new pipeline. Please provide the details."})
                st.rerun()
        
        else:
            st.session_state.last_prompt_is_get = False
        
        # Proceed with conversational flow if no command was found
        if st.session_state.conversation_stage == "completed":
            st.session_state.messages.append({"role": "assistant", "content": "I'm sorry, I couldn't understand that command. Would you like to `create` a new pipeline, `show table`, or `show details`?"})
            st.session_state.conversation_stage = "initial"
            st.rerun()
        
        if st.session_state.conversation_stage != "completed" and not st.session_state.last_prompt_is_get:
            current_data = {}
            table_type = ""
            prompt_key = ""

            if st.session_state.conversation_stage == "in_progress":
                st.session_state.conversation_stage = "header_in_progress"
                st.session_state.messages.append({"role": "assistant", "content": "Okey, Please provide the details for the Pipeline table details."})
                st.rerun()
            
            if st.session_state.conversation_stage == "header_in_progress":
                current_data = st.session_state.pipeline_data['header']
                table_type = "header"
                prompt_key = "header"
            elif st.session_state.conversation_stage == "detail_l0_in_progress":
                current_data = st.session_state.pipeline_data['detail']
                table_type = "l0"
                prompt_key = "l0"
            elif st.session_state.conversation_stage == "l0_num_tables_in_progress":
                try:
                    num_tables = int(prompt.strip())
                    if 1 <= num_tables <= 5:
                        st.session_state.pipeline_data['header']['no_of_tables'] = num_tables
                        st.session_state.pipeline_data['detail'] = [{} for _ in range(num_tables)]
                        st.session_state.conversation_stage = "detail_l0_in_progress"
                        st.session_state.messages.append({"role": "assistant", "content": f"Okay, now provide details for the {num_tables} L0 tables. You can specify the table number, e.g., 'table1 source is my_source'."})
                    else:
                        st.markdown("Please enter a number between 1 and 5.")
                        st.session_state.messages.append({"role": "assistant", "content": "Please enter a number between 1 and 5."})
                    st.rerun()
                except ValueError:
                    st.markdown("Invalid input. Please enter a number.")
                    st.session_state.messages.append({"role": "assistant", "content": "Invalid input. Please enter a number."})
                    st.rerun()
            elif st.session_state.conversation_stage == "detail_pb_in_progress":
                current_data = st.session_state.pipeline_data['detail']
                table_type = "pb"
                prompt_key = "l1_l2"

            with st.chat_message("assistant"):
                if st.session_state.conversation_stage in ["header_in_progress", "detail_l0_in_progress", "detail_pb_in_progress"]:
                    prompt_to_parse = prompt
                    if prompt.lower().strip().startswith("create"):
                        prompt_to_parse = prompt[len("create"):].strip()

                    full_prompt = f"{SYSTEM_PROMPTS[prompt_key]}\nUser input: {prompt_to_parse}"
                    response = model.generate_content(full_prompt)

                    try:
                        response_text = response.text.replace("```json", "").replace("```", "").strip()
                        if not response_text.strip() or not response_text.startswith('{') or not response_text.endswith('}'):
                            raise ValueError("Invalid response format from the model. Expected a JSON object.")
                        
                        extracted_data = json.loads(response_text)
                        
                        # Apply case-insensitive updates
                        updated_data = {k: v for k, v in extracted_data.items()}

                        if table_type == "header":
                            st.session_state.pipeline_data['header'].update(updated_data)
                            check_and_transition_stage() 
                        elif table_type == "pb":
                            st.session_state.pipeline_data['detail'].update(updated_data)
                            check_and_transition_stage() 
                        elif table_type == "l0":
                            table_index = 0
                            for map_key, index in L0_TABLE_MAP.items():
                                if map_key in prompt.lower():
                                    table_index = index
                                    break
                            
                            if isinstance(st.session_state.pipeline_data.get('detail'), list) and len(st.session_state.pipeline_data['detail']) > table_index:
                                st.session_state.pipeline_data['detail'][table_index].update(updated_data)
                                check_and_transition_stage() 
                            else:
                                st.markdown("Please specify which L0 table you are providing details for (e.g., 'table1').")
                                st.session_state.messages.append({"role": "assistant", "content": "Please specify which L0 table you are providing details for (e.g., 'table1')."})
                                st.rerun()

                        check_and_transition_stage()

                    except (json.JSONDecodeError, ValueError) as e:
                        error_message = f"I'm sorry, I couldn't understand that. The AI did not provide a valid data response. Please try to provide the information in a clear format."
                        st.markdown(error_message)
                        st.session_state.messages.append({"role": "assistant", "content": error_message})
                        st.rerun()
                    except Exception as e:
                        st.error(f"An unexpected error occurred: {e}")
                        st.session_state.messages.append({"role": "assistant", "content": "An unexpected error occurred. Please try again."})
                        st.rerun()                   