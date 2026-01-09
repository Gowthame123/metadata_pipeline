
import streamlit as st
import search
import add_edit
import ai_assistant
from collections import defaultdict
import uuid
import database
from description import description_page
# import base64
# from pathlib import Path
# from PIL import Image

if 'theme' not in st.session_state:
    st.session_state.theme = 'dark' 

# 2. Set the Streamlit theme base based on session state.
# This must happen before any component is rendered.
# We use the internal config setter which is more direct for the base theme.
try:
    st._config.set_option('theme.base', st.session_state.theme)
except Exception as e:
    # Fallback/Ignore if st._config is not directly accessible in the environment
    pass

# # Function to get the Base64 string of the logo file
# def get_base64_of_bin_file(bin_file):
#     with open(bin_file, 'rb') as f:
#         data = f.read()
#     return base64.b64encode(data).decode()

# # Function to build the CSS and HTML for the logo
# def build_logo_html(logo_path, width="o.5px", bottom="5px", right="5px"):
#     try:
#         base64_logo = get_base64_of_bin_file(logo_path)
#         return f"""
#             <style>
#                 .logo-container {{
#                     position: fixed;
#                     bottom: {bottom};
#                     right : {right};
#                     z-index: 999;
#                 }}
#             </style>
#             <div class="logo-container">
#                 <img src="data:image/png;base64,{base64_logo}" width="{width}">
#             </div>
#         """
#     except FileNotFoundError:
#         return ""

# ---- Main App Code ----

# Set page configuration
st.set_page_config(layout="wide", page_title="Metaflow (metadata powered pipeline)", page_icon="logo.jpg")


# Initialize the database
database.init_db()
if 'theme' not in st.session_state:
    st.session_state.theme = 'Dark' if st.session_state.get('dark_mode', False) else 'Light'
def load_pipeline_for_editing(pipeline_id):
    """
    Callback function to fetch pipeline data and set session state for editing.
    """
    pipeline_data = database.get_pipeline_by_id(pipeline_id)
    if pipeline_data:
        # Store the entire data object in session state
        st.session_state['edit_data'] = pipeline_data
        st.session_state.edit_pipeline_id = pipeline_id
        st.session_state.current_view = 'add_edit'
        st.session_state.form_visible = True
        st.session_state.current_pipeline_layer = pipeline_data['ETL_LAYER']
    else:
        st.error("Pipeline not found.")
        st.session_state.edit_pipeline_id = None

# Initialize session state for pipelines
if 'pipelines' not in st.session_state:
    st.session_state['pipelines'] = database.get_all_pipelines()

# Initialize other session state variables
if 'current_view' not in st.session_state:
    st.session_state['current_view'] = 'search'
if 'edit_pipeline_id' not in st.session_state:
    st.session_state['edit_pipeline_id'] = None
if 'current_pipeline_layer' not in st.session_state:
    st.session_state['current_pipeline_layer'] = "L0"
if 'owner' not in st.session_state:
    st.session_state['owner'] = ""    
if 'form_visible' not in st.session_state:
    st.session_state['form_visible'] = False
if 'ai_collected_data' not in st.session_state:
    st.session_state['ai_collected_data'] = None
if 'edit_data' not in st.session_state:
    st.session_state['edit_data'] = None
# if 'theme' not in st.session_state:
#     st.session_state.theme = 'light'
# Header and Navigation
col1, col2, col3 = st.columns([4, 0.8, 0.2])
with col1:
    st.title("Metaflow (metadata powered pipeline)")
with col2:
    if st.button("📑Guide", use_container_width=True, type="primary"):
        st.session_state.current_view = 'description'
        st.session_state.form_visible = False
        st.rerun()
with col3:
        # Custom theme toggle using a button
        if st.session_state.theme == 'light':
            button_label = "🌙"
            new_theme = 'dark'
        else:
            button_label = "☀️"
            new_theme = 'light'

        if st.button(button_label):
            st.session_state.theme = new_theme
            # Set Streamlit's internal theme option
            st.config.set_option("theme.base", new_theme)
            st.rerun() # Rerun to apply the theme change immediately

# Apply the current theme setting
st.config.set_option("theme.base", st.session_state.theme)
        
col1, col2, col3 = st.columns([4, 0.5, 0.5])

with col1:
    st.write("Create, search, and manage metadata for your Databricks data pipelines")
with col2:
    if st.button("🤖 New AI Pipeline ", use_container_width=True, type="primary"):
        st.session_state.current_view = 'ai_assistant'
        st.session_state.form_visible = True  
        st.rerun() 
# with col3:
#     if st.button("➕ New Pipeline", use_container_width=True, type="primary"):
#         st.session_state.current_view = 'add_edit'
#         st.session_state.form_visible = True
#         st.session_state.edit_pipeline_id = None
#         st.session_state.current_pipeline_layer = "L0"
#         st.rerun()

with col3:
    if st.button("➕ New Pipeline", use_container_width=True, type="primary"):
        st.session_state.current_view = 'add_edit'
        st.session_state.form_visible = True
        st.session_state.edit_pipeline_id = None
        st.session_state.current_pipeline_layer = "L0"
        
        # --- FIX: FULLY RESET THE FORM-RELATED SESSION STATE KEYS ---
        st.session_state['general_data'] = {}
        st.session_state['l0_tables_data'] = [{}]
        st.session_state['num_tables'] = 1
        st.session_state['pb_data'] = {}
        # You may also need to clear individual widget keys if you're using them
        # for i in range(5):
        #    st.session_state.pop(f"source_{i}", None)
        #    ... and so on for all L0 and L1/L2 keys
        # The simplest way is to clear the parent dictionaries
        
        st.rerun()

col1, col2, col3 = st.columns([1, 1, 1])

with col1:
    if st.button("🔍 Search Pipelines", use_container_width=True):
        st.session_state.current_view = 'search'
with col2:
    if st.button("📝 Add/Edit Pipeline", use_container_width=True):
        st.session_state.current_view = 'add_edit'
        st.session_state.form_visible = False
        st.session_state.edit_pipeline_id = None
        st.session_state.current_pipeline_layer = "L0"
        st.session_state['edit_data'] = None
with col3:
    if st.button("🤖 AI Assistant", use_container_width=True):
        st.session_state.current_view = 'ai_assistant'


st.markdown("---")

# Main content based on current view
if st.session_state.current_view == 'search':
    search.show()
elif st.session_state.current_view == 'add_edit':
    prefill_data = st.session_state.pop('ai_collected_data', None)
    if prefill_data:
        add_edit.show(prefill_data=prefill_data)
    else:
        add_edit.show()
elif st.session_state.current_view == 'ai_assistant':
    ai_assistant.show()
elif st.session_state.current_view == 'description':
    description_page()
