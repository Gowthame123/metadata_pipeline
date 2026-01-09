import sqlite3
import datetime

def init_db():
    """
    Initializes the SQLite database and creates the necessary tables.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_control_header (
            DATA_FLOW_GROUP_ID STRING PRIMARY KEY,
            BUSINESS_UNIT STRING,
            PRODUCT_OWNER STRING,
            TRIGGER_TYPE STRING,
            BUSINESS_OBJECT_NAME STRING,
            ETL_LAYER STRING,
            COMPUTE_CLASS STRING,
            COMPUTE_CLASS_DEV STRING,
            DATA_SME STRING,
            INGESTION_MODE STRING,
            INGESTION_BUCKET STRING,
            SPARK_CONFIGS STRING,
            COST_CENTER STRING,
            WARNING_THRESHOLD_MINS INT,
            WARNING_DL_GROUP STRING,
            min_version REAL,
            max_version REAL,
            IS_ACTIVE STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,
            INSERTED_TS STRING,
            UPDATED_TS STRING
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_l0_detail (
            DATA_FLOW_GROUP_ID STRING,
            SOURCE STRING NOT NULL,
            SOURCE_OBJ_SCHEMA STRING NOT NULL,
            SOURCE_OBJ_NAME STRING NOT NULL,
            INPUT_FILE_FORMAT STRING,
            STORAGE_TYPE STRING,
            CUSTOM_SCHEMA STRING,
            DELIMETER STRING,
            DQ_LOGIC STRING,
            CDC_LOGIC STRING,
            TRANSFORM_QUERY STRING,
            LOAD_TYPE STRING,
            PRESTAG_FLAG STRING,
            PARTITION STRING,
            LS_FLAG STRING,
            LS_DETAIL STRING,
            LOB STRING,
            IS_ACTIVE STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,      
            FOREIGN KEY (DATA_FLOW_GROUP_ID) REFERENCES data_flow_control_header(DATA_FLOW_GROUP_ID),
            UNIQUE (DATA_FLOW_GROUP_ID, SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME)
        )
    """)
    
    # Removed UNIQUE (DATA_FLOW_GROUP_ID) constraint from data_flow_pb_detail 
    # to allow for update/insert logic without an explicit pb_id, assuming 
    # a 1:1 or 1:N relationship where the N side is managed by DATA_FLOW_GROUP_ID
    # I am assuming a 1:1 relationship based on the original update_pb_details logic.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_pb_detail (
            DATA_FLOW_GROUP_ID STRING,
            TARGET_OBJ_SCHEMA STRING,
            TARGET_OBJ_NAME STRING,
            PRIORITY INT,
            TARGET_OBJ_TYPE STRING,
            TRANSFORM_QUERY STRING,
            GENERIC_SCRIPTS STRING,
            SOURCE_PK STRING,
            TARGET_PK STRING,
            LOAD_TYPE STRING,
            PARTITION_METHOD STRING,
            PARTITION_OR_INDEX STRING,
            CUSTOM_SCRIPT_PARAMS STRING,
            RETENTION_DETAILS STRING,
            LOB STRING,
            IS_ACTIVE STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,
            FOREIGN KEY (DATA_FLOW_GROUP_ID) REFERENCES data_flow_control_header(DATA_FLOW_GROUP_ID)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_cluster_config_lookup (
            COMPUTE_CLASS STRING PRIMARY KEY,
            DESCRIPTION STRING,
            MIN_WORKER INT,
            MAX_WORKER INT,
            DRIVER_NODE_TYPE_ID STRING,
            WORKER_NODE_TYPE_ID STRING,
            RUNTIME_ENGINE STRING,
            SPARK_VERSION STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,
            INSERTED_TS TEXT,
            UPDATED_TS TEXT,
            DEV_ALLOWED STRING
        )
    """)

    conn.commit()
    conn.close()
    
    _seed_cluster_config_data()

def _seed_cluster_config_data():
    """
    Seeds the data_flow_cluster_config_lookup table with hardcoded data if it's empty.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM data_flow_cluster_config_lookup")
    if cursor.fetchone()[0] > 0:
        conn.close()
        return

    data = [
        ('M_M6', 'M_M6', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_M6', 'L_M6', 4, 16, None, None, None, None, None, None, None, None, 'Y'),
        ('S_M6', 'S_M6', 1, 4, None, None, None, None, None, None, None, None, 'Y'),
        ('XL_M6', 'XL_M6', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('S_I3', 'S_I3', 1, 4, None, None, 'x', None, None, None, None, None, 'Y'),
        ('M_I3', 'M_I3', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_I3', 'L_I3', 4, 16, None, None, None, None, None, None, None, None, 'Y'),
        ('XL_I3', 'XL_I3', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('S_R5', 'S_R5', 1, 4, None, None, None, None, None, None, None, None, 'Y'),
        ('M_R5', 'M_R5', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_R5', 'L_R5', 4, 16, None, None, None, None, None, None, None, None, 'N'),
        ('XL_R5', 'XL_R5', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('S_C5', 'S_C5', 1, 4, None, None, None, None, None, None, None, None, 'Y'),
        ('M_C5', 'M_C5', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_C5', 'L_C5', 4, 16, None, None, None, None, None, None, None, None, 'Y'),
        ('XL_C5', 'XL_C5', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('XXL_C5', 'XXL_C5', 16, 64, None, None, None, None, None, None, None, None, 'N'),
        ('Serverless', 'Serverless', None, None, None, None, None, None, None, None, None, None, 'Y'),
        ('S_R5_WP', 'S_R5_WP', 1, 4, None, None, None, None, None, None, None, None, 'N'),
        ('M_R5_WP', 'M_R5_WP', 2, 8, None, None, None, None, None, None, None, None, 'N'),
        ('L_R5_WP', 'L_R5_WP', 4, 16, None, None, None, None, None, None, None, None, 'N'),
        ('XL_R5_WP', 'XL_R5_WP', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('XXL_R5_WP', 'XXL_R5_WP', 16, 64, None, None, None, None, None, None, None, None, 'N'),
        ('S_C6_WP', 'S_C6_WP', 1, 4, None, None, None, None, None, None, None, None, 'N'),
        ('M_C6_WP', 'M_C6_WP', 2, 8, None, None, None, None, None, None, None, None, 'N'),
        ('L_C6_WP', 'L_C6_WP', 4, 16, None, None, None, None, None, None, None, None, 'N'),
        ('XL_C6_WP', 'XL_C6_WP', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('XXL_C6_WP', 'XXL_C6_WP', 16, 64, None, None, None, None, None, None, None, None, 'N')
    ]

    cursor.executemany("""
        INSERT INTO data_flow_cluster_config_lookup (
            COMPUTE_CLASS, DESCRIPTION, MIN_WORKER, MAX_WORKER, DRIVER_NODE_TYPE_ID,
            WORKER_NODE_TYPE_ID, RUNTIME_ENGINE, SPARK_VERSION, INSERTED_BY, UPDATED_BY,
            INSERTED_TS, UPDATED_TS, DEV_ALLOWED
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

def save_general_info(general_data):
    """Inserts a new header record."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    general_data['INSERTED_TS'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    general_data['UPDATED_TS'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    columns = ', '.join(general_data.keys())
    placeholders = ', '.join('?' * len(general_data))
    values = tuple(general_data.values())
    
    cursor.execute(f"INSERT INTO data_flow_control_header ({columns}) VALUES ({placeholders})", values)
    conn.commit()
    conn.close()

def update_general_info(general_data, data_flow_group_id):
    """Updates an existing header record."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    general_data['UPDATED_TS'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Exclude primary key from update columns
    data_to_update = {k: v for k, v in general_data.items() if k != 'DATA_FLOW_GROUP_ID'}
    
    columns = data_to_update.keys()
    values = tuple(data_to_update.values())
    
    update_cols = ', '.join([f"{col} = ?" for col in columns])
    
    cursor.execute(f"UPDATE data_flow_control_header SET {update_cols} WHERE DATA_FLOW_GROUP_ID = ?", values + (data_flow_group_id,))
    conn.commit()
    conn.close()


def save_l0_details(l0_data_list, data_flow_group_id):
    """
    Saves a list of L0 detail records for a given pipeline header.
    Performs inserts for new records using INSERT OR IGNORE to handle the unique constraint.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    for l0_data in l0_data_list:
        data_to_save = l0_data.copy()
        data_to_save['DATA_FLOW_GROUP_ID'] = data_flow_group_id

        columns = ', '.join(data_to_save.keys())
        placeholders = ', '.join('?' * len(data_to_save))
        values = tuple(data_to_save.values())

        # Use INSERT OR IGNORE based on the unique constraint (DATA_FLOW_GROUP_ID, SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME)
        cursor.execute(f"INSERT OR IGNORE INTO data_flow_l0_detail ({columns}) VALUES ({placeholders})", values)

    conn.commit()
    conn.close()

def update_l0_details(l0_data_list, data_flow_group_id):
    """
    Updates existing L0 detail records based on the composite unique key.
    Performs an UPDATE if a record exists; performs an INSERT otherwise.
    (Note: Deletion logic is removed as it relied on l0_id).
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    for l0_data in l0_data_list:
        
        # Keys that form the unique identifier
        unique_keys = ['DATA_FLOW_GROUP_ID', 'SOURCE', 'SOURCE_OBJ_SCHEMA', 'SOURCE_OBJ_NAME']
        
        # Prepare data for update/insert
        data_to_update = {k: v for k, v in l0_data.items() if k not in unique_keys}
        data_to_update['DATA_FLOW_GROUP_ID'] = data_flow_group_id # Ensure FK is present for update/insert

        # Attempt to UPDATE first
        update_cols = ', '.join([f"{col} = ?" for col in data_to_update.keys()])
        update_values = tuple(data_to_update.values()) + (
            data_flow_group_id, 
            l0_data['SOURCE'], 
            l0_data['SOURCE_OBJ_SCHEMA'], 
            l0_data['SOURCE_OBJ_NAME']
        )
        
        cursor.execute(f"""
            UPDATE data_flow_l0_detail 
            SET {update_cols} 
            WHERE DATA_FLOW_GROUP_ID = ? AND SOURCE = ? AND SOURCE_OBJ_SCHEMA = ? AND SOURCE_OBJ_NAME = ?
        """, update_values)
        
        # If no rows were updated, INSERT the record
        if cursor.rowcount == 0:
            full_data = l0_data.copy()
            full_data['DATA_FLOW_GROUP_ID'] = data_flow_group_id
            
            columns = ', '.join(full_data.keys())
            placeholders = ', '.join('?' * len(full_data))
            values = tuple(full_data.values())
            
            # Use INSERT OR IGNORE to respect the unique constraint on the detail table
            cursor.execute(f"INSERT OR IGNORE INTO data_flow_l0_detail ({columns}) VALUES ({placeholders})", values)

    conn.commit()
    conn.close()

def save_pb_details(pb_data, data_flow_group_id):
    """Inserts a single L1/L2 detail record."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    pb_data['DATA_FLOW_GROUP_ID'] = data_flow_group_id
    columns = ', '.join(pb_data.keys())
    placeholders = ', '.join('?' * len(pb_data))
    values = tuple(pb_data.values())

    # INSERT OR REPLACE handles the case where there is a unique constraint on DATA_FLOW_GROUP_ID 
    # (even though I removed the explicit unique constraint, this is safer for 1:1 records).
    cursor.execute(f"INSERT OR REPLACE INTO data_flow_pb_detail ({columns}) VALUES ({placeholders})", values)
    conn.commit()
    conn.close()
    
def update_pb_details(pb_data, data_flow_group_id):
    """Updates an existing L1/L2 detail record based on DATA_FLOW_GROUP_ID."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    # Exclude foreign key from update list, but use it in WHERE clause
    data_to_update = {k: v for k, v in pb_data.items() if k != 'DATA_FLOW_GROUP_ID'}

    columns = data_to_update.keys()
    values = tuple(data_to_update.values())

    update_cols = ', '.join([f"{col} = ?" for col in columns])

    # Update based only on DATA_FLOW_GROUP_ID, assuming one PB detail record per header.
    cursor.execute(f"UPDATE data_flow_pb_detail SET {update_cols} WHERE DATA_FLOW_GROUP_ID = ?", values + (data_flow_group_id,))

    # If no row was updated, it means the record doesn't exist, so insert it.
    if cursor.rowcount == 0:
        save_pb_details(pb_data, data_flow_group_id)

    conn.commit()
    conn.close()


def get_all_pipelines():
    """
    Fetches all pipelines by selecting only from the header table.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM data_flow_control_header ORDER BY UPDATED_TS DESC")
    headers = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
    conn.close()
    return headers

def get_pipeline_by_id(data_flow_group_id):
    """Fetches a single pipeline and its detail records by ID."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    columns = [col[0] for col in cursor.description]
    pipeline_data = cursor.fetchone()

    if not pipeline_data:
        conn.close()
        return None

    pipeline_dict = dict(zip(columns, pipeline_data))

    cursor.execute("SELECT * FROM data_flow_l0_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    l0_columns = [col[0] for col in cursor.description]
    l0_details = [dict(zip(l0_columns, row)) for row in cursor.fetchall()]
    pipeline_dict['l0_details'] = l0_details

    cursor.execute("SELECT * FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    pb_columns = [col[0] for col in cursor.description]
    pb_details = [dict(zip(pb_columns, row)) for row in cursor.fetchall()]
    pipeline_dict['pb_details'] = pb_details
    
    conn.close()
    return pipeline_dict

def delete_pipeline(data_flow_group_id):
    """Deletes a complete pipeline and all its associated records."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM data_flow_l0_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        cursor.execute("DELETE FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        cursor.execute("DELETE FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error deleting pipeline: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_compute_classes(dev_allowed=False):
    """
    Fetches COMPUTE_CLASS options from the lookup table.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    if dev_allowed:
        query = "SELECT COMPUTE_CLASS FROM data_flow_cluster_config_lookup WHERE DEV_ALLOWED = 'Y';"
    else:
        query = "SELECT COMPUTE_CLASS FROM data_flow_cluster_config_lookup;"
        
    cursor.execute(query)
    classes = [row[0] for row in cursor.fetchall() if row[0] is not None]
    conn.close()
    
    return sorted(list(set(classes)))