import streamlit as st
import os

def description_page():
    st.title("📑Guide to Metadata Pipeline Fields")

    # Data from info_context.txt
    # This dictionary is created manually based on the provided text
    data = {
        "DATAFLOW_NAME_HELP": "The unique identifier for the data flow group. This ID must be unique across all pipelines and layers.",
        "BUSINESS_UNIT_HELP": "The business unit that owns or is responsible for this data flow.",
        "PRODUCT_OWNER_HELP": "The email or name of the product owner for the pipeline.",
        "STATUS_HELP": "Indicates if the pipeline is active ('Y') or inactive ('N'). An inactive pipeline will not run.",
        "STATUS_HELP_2": "Indicates if the pipeline is active ('Y') or inactive ('N'). An inactive pipeline will not run.",        
        "WARNING_THRESHOLD_HELP": "The maximum time in minutes allowed for a pipeline run before a warning is triggered.",
        "TRIGGER_TYPE_HELP": "The method used to trigger the pipeline, either as a 'DLT' (Delta Live Tables) pipeline or a standard 'JOB'.",
        "ETL_LAYER_HELP": "The layer in the data architecture for this pipeline. L0: Raw, L1: Curated, L2: Data Product.",
        "DATA_SME_HELP": "The Subject Matter Expert who is knowledgeable about the data in this pipeline.",
        "INGESTION_MODE_HELP": "The method used for ingesting data. 'EXTL_FULL', 'EXTL_INC', 'DATASPHERE_INGEST', 'API_INGEST', 'DB_INGEST'",
        "INGESTION_BUCKET_HELP": "The cloud storage bucket or data lake path where the raw data is ingested.",
        "BUSINESS_OBJECT_NAME_HELP": "The business-friendly name for the data object or entity processed by this pipeline.",
        "COMPUTE_CLASS_HELP": "The compute cluster configuration for production environments.",
        "COMPUTE_CLASS_DEV_HELP": "The compute cluster configuration for development and testing environments.",
        "COST_CENTER_HELP": "The cost center or budget code associated with the resources for this pipeline.",
        "WARNING_DL_GROUP_HELP": "The distribution list (email group) to notify in case of warnings or pipeline failures.",
        "SPARK_CONFIGS_HELP": "Additional Apache Spark configuration parameters as a key-value string (e.g., 'spark.sql.shuffle.partitions=10').",
        "MIN_VERSION_HELP": "The minimum required version of the framework, library, or platform to run this pipeline.",
        "MAX_VERSION_HELP": "The maximum compatible version of the framework, library, or platform.",
        "INSERTED_BY_HELP": "The user ID of the person who initially created this pipeline record.",
        "UPDATED_BY_HELP": "The user ID of the person who last updated this pipeline record.",
        "SOURCE_HELP": "The source system or application from which the data is pulled (e.g., 'Salesforce', 'SAP', 'S3_Bucket').",
        "SOURCE_OBJ_SCHEMA_HELP": "The schema of the source object (e.g., database schema, folder path in S3).",
        "SOURCE_OBJ_NAME_HELP": "The name of the source object (e.g., table name, view name, file name).",
        "INPUT_FILE_FORMAT_HELP": "The format of the input data files (e.g., 'csv', 'parquet', 'json', 'delta').",
        "STORAGE_TYPE_HELP": "The type of storage: 'C' for columnar (file-like). C1, C2, C3, C4",
        "DELIMETER_HELP": "The character used to separate fields in a delimited file (e.g., ',' for CSV).",
        "CUSTOM_SCHEMA_HELP": "A custom schema definition for the source data, typically in JSON format.",
        "DQ_LOGIC_HELP": "The Data Quality checks and rules applied to the ingested data to ensure its validity.",
        "CDC_LOGIC_HELP": "The logic for handling Change Data Capture (CDC) to process incremental changes from the source.",
        "TRANSFORM_QUERY_HELP": "The SQL or other query logic used to transform the data.",
        "LOAD_TYPE_HELP": "The data load strategy: 'FULL' for a full refresh or 'DELTA' for an incremental load and 'SCD' and 'PySpark'.",
        "PRESTAG_FLAG_HELP": "A flag to indicate if data should be pre-staged in a temporary location before the main transformation. 'Y' or 'N'.",
        "PARTITION_HELP": "The column or expression used for partitioning the target table to optimize queries.",
        "LOB_HELP": "The Line of Business associated with the data.",
        "TARGET_OBJ_TYPE_HELP": "The type of the target data object: 'Table', 'MV' (Materialized View), or 'View'.",
        "SOURCE_PK_HELP": "The primary key of the source object.",
        "TARGET_OBJ_SCHEMA_HELP": "The schema where the target object will reside.",
        "PRIORITY_HELP": "The priority level of the pipeline: 'Low', 'Medium', 'High', or 'Critical'.",
        "GENERIC_SCRIPTS_HELP": "A path or list of generic scripts to be executed as part of the pipeline.",
        "TARGET_PK_HELP": "The primary key of the target object.",
        "PARTITION_METHOD_HELP": "The method used for partitioning or clustering the target table (e.g., 'Partition' or 'Liquid cluster').",
        "PARTITION_OR_INDEX_HELP": "The specific column or index used for partitioning or clustering.",
        "CUSTOM_SCRIPT_PARAMS_HELP": "Parameters for any custom scripts that need to be run.",
        "RETENTION_DETAILS_HELP": "Details on the data retention policy for the target object (e.g., '30 days', '1 year').",
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

    # Display data using st.markdown
    # We will use the 'data' dictionary which has all the descriptions.
    # The FIELD_MAPPING is used to get the user-friendly names.

    # Function to get the correct key from the manually created dictionary
    # The source text has some minor variations, so we need a helper function.
    def get_description(original_key):
        if f"{original_key}_HELP" in data:
            return data[f"{original_key}_HELP"]
        # Handle the specific case where status has two parts
        if original_key == 'DATA_FLOW_GROUP_ID':
            return data['DATAFLOW_NAME_HELP']
        if original_key == 'STATUS':
            return f"{data['STATUS_HELP']} {data['STATUS_HELP_2']}"
        # Handle the case for delimiter
        if original_key == 'DELIMETER':
            return data['DELIMETER_HELP']
        # Handle the case for partition
        if original_key == 'PARTITION':
            return data['PARTITION_HELP']
        return "Description not found."

    for original_key, friendly_name in FIELD_MAPPING.items():
        description = get_description(original_key)
        markdown_content = f"### :blue[{friendly_name.title()}]\n\n{description}"
        st.markdown(markdown_content)

    # Add a "Back" button to return to the previous view
    if st.button("⬅️ Back"):
        st.session_state.current_view = 'search'
        st.rerun()