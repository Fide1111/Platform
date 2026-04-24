from datetime import datetime
from pandas import ExcelWriter
from pathlib import Path
from function import report_claims_triangle, report_claims_reserve, report_ibnr_projection, report_travel_analysis, update_db, checking_claim_triangle
import os
import sqlite3
import pandas as pd
import streamlit as st # type: ignore
import time

BASE_DIR = Path(__file__).parent.resolve()
DIRECT_DATA_DB_PATH = os.path.join(os.getcwd(), "direct_data.db")
FAC_IN_DATA_DB_PATH = os.path.join(os.getcwd(), "fac_in_data.db")   
LOGO_PATH = BASE_DIR / "pic" / "fidelidade-logo.png"


# Current Time
timestamp = datetime.now().strftime("%Y-%m-%d")

# Logo
st.logo(str(LOGO_PATH), size='large')

st.markdown(
    """
    <style>
        [alt="Logo"] {          
            height: 80px !important;   
            width: auto !important;    
        }
    </style>
    """,
    unsafe_allow_html=True
)

if 'generated_dfs' not in st.session_state:
    st.session_state.generated_dfs = None
if 'generated_success' not in st.session_state:
    st.session_state.generated_success = False

# Default Setting
years = range(2010,2027)

list_of_lob = {
    "2111": "EC",
    "2121": "Individual_PA",
    "2122": "Group_PA_excl_Student_PA)",
    "2123": "Travel",
    "222": "Hospital_Cash_Plan",
    "223": "Group_Medical",
    "311": "Fire_Insurance",
    "323": "Burglary",
    "3261": "Machinery",
    "3262": "Electronic_Equipment",
    "3263": "Contractor_Plant",
    "32711": "Household",
    "3273": "PAR",
    "3283": "CAR",
    "41": "Motor_Damage_to_Vehicle",
    "43": "Motor_TPL", 
    "44": "Motor_Passenger_Liability",
    "62": "Aviation",
    "71": "Marine_Exh_All_Risks",
    "72": "Inland_Transit_Insurance",
    "79": "Marine_Cargo_Exh_All_Risks",
    "82": "Prof_Liab_excl_Lawyers_Liab",
    "83": "Cyber_Insurance",
    "854": "Public_Liability",
    "92": "Fidelity_Guarantee",
}

sap_options = list(list_of_lob.keys())
sap_display = [f"{code} - {name}" for code, name in list_of_lob.items()]

DIRECT_DATA_DB_PATH = os.path.join(os.getcwd(), "direct_data.db")
FAC_IN_DATA_DB_PATH = os.path.join(os.getcwd(), "fac_in_data.db")   


@st.cache_data
def load_data():
    empty_df = pd.DataFrame()
    if not os.path.exists(DIRECT_DATA_DB_PATH) or not os.path.exists(FAC_IN_DATA_DB_PATH):
        st.error("Database file not found!")
        return pd.DataFrame()
    with sqlite3.connect(DIRECT_DATA_DB_PATH) as conn_d:
        direct_data = pd.read_sql_query("SELECT * FROM direct_data", conn_d)
    
    with sqlite3.connect(FAC_IN_DATA_DB_PATH) as conn_f:
        fac_in_data = pd.read_sql_query("SELECT * FROM fac_in_data", conn_f)
    return direct_data, fac_in_data
direct_data, fac_in_data = load_data()

# Page Setting
if "current_page" not in st.session_state:
    st.session_state.current_page = "Main Page"

st.sidebar.title("Navigation")

page_options = [
    "Main Page",
    "Update DB",
    "Generate Claim Triangle Report",
    "Generate Claim Reserves Report",
    "Generate IBNR Projection Report"
    "Geneerate Travel Claim Analysis Report"
]

selected_page = st.sidebar.selectbox(
    "Select what to do",
    options=page_options,
    index=page_options.index(st.session_state.current_page),
    key="sidebar_select"
)

if selected_page != st.session_state.current_page:
    st.session_state.current_page = selected_page
    st.rerun()



if st.session_state.current_page == "Main Page":
    st.markdown("### This is the Main Page")
    st.write("Use the sidebar to switch between pages.")

elif st.session_state.current_page == "Update DB":
    st.title("Update Database with New Claims Data")
    
    uploaded_file = st.file_uploader(
        "Choose Excel file",
        type=["xlsx", "xls"],
        accept_multiple_files=False,
        help="File should contain sheets: 'Direct Ceded' and 'Accepted-Retro '"
    )

    update_button = st.button(
        "Update DB Now", 
        type="primary", 
        disabled=(uploaded_file is None)
    )
    if update_button:
        with st.spinner('Updating database...'):
            try:
                update_db(uploaded_file)
                st.cacue_data.clear()
                st.success("Database updated.")
            except Exception as e:
                st.error(f"Error Updating database: {e}")
        
    st.subheader("Check if the data updated correctly")
    df_direct, df_fac_in = checking_claim_triangle(r"C:\Users\F170\Desktop\AA Report\Function_Claims_Triangle\template\claims_register_new.xlsx")

elif st.session_state.current_page == "Generate Claim Triangle Report":
    st.title("Generate Claim Triangle Report")
    output_claim_triangle_report = 'claims_triangle.xlsx'
    generate_claim_triangle_button = st.button(
        "Generate Report Now",
        type = 'primary'
    )
    if generate_claim_triangle_button:
        with st.spinner("Generateing Report..."):
            try:
                report_claims_triangle(direct_data, fac_in_data)

                with open(output_claim_triangle_report, 'rb') as f:
                    file_data = f.read()
                st.success("Report Generated.")
                st.download_button(
                    label="📥 Download Excel Report",
                    data=file_data,
                    file_name=f"Claims_Triangle_Report_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"Error Generate Report: {e}")
    
elif st.session_state.current_page == "Generate Claim Reserves Report":
    st.title("Generate Claim Triangle Report")
    output_claim_reserves_report = 'claims_reserves.xlsx'
    generate_claim_reserves_button = st.button(
        "Generate Report Now",
        type = 'primary'
    )
    if generate_claim_reserves_button:
        with st.spinner("Generateing Report..."):
            try:
                report_claims_reserve()

                with open(output_claim_reserves_report, 'rb') as f:
                    file_data = f.read()
                st.success("Report Generated.")
                st.download_button(
                    label="📥 Download Excel Report",
                    data=file_data,
                    file_name=f"Claims_Reserves_Report_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"Error Generate Report: {e}")

elif st.session_state.current_page == "Generate IBNR Projection Report":
    st.title("Generate Claim Triangle Report")
    output_ibnr_projection_report = 'ibnr_projection.xlsx'

    options = ["EC", "Income Protection", "Medical Expenses", "Fire", "Other Motor",
               "Motor TPL", "Transport", "General Liability"]

    if st.button("🚀 Generate All Reports", type='primary'):
            # Initialize dictionary to hold file data
            st.session_state.generated_files = {} 
            
            progress_text = "Batch processing in progress. Please wait."
            my_bar = st.progress(0, text=progress_text)
            
            try:
                for index, option in enumerate(options):
                    # Update progress
                    percent_complete = (index + 1) / len(options)
                    my_bar.progress(percent_complete, text=f"Processing: {option}")
                    
                    # Run the function and get the filename
                    file_path = report_ibnr_projection(option)
                    
                    # Read the file into memory immediately
                    with open(file_path, 'rb') as f:
                        st.session_state.generated_files[option] = f.read()
                
                my_bar.empty()
                st.success("✅ All 8 reports generated successfully!")
                st.balloons()
            except Exception as e:
                st.error(f"❌ Error: {e}")

            # 3. The Download Gallery
            if "generated_files" in st.session_state:
                st.write("### 📂 Download Center")
                for product, file_bytes in st.session_state.generated_files.items():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.info(f"Report: {product}")
                    with col2:
                        st.download_button(
                            label="Download",
                            data=file_bytes,
                            file_name=f"IBNR_{product.replace(' ', '_')}_{timestamp}.xlsm",
                            mime="application/vnd.ms-excel.sheet.macroEnabled.12",
                            key=f"btn_{product}"
                        )

elif st.session_state.current_page == "Generate Travel Claim Analysis Report":
    st.title("Generate Travel Claim Analysis Report")
    
    st.write("Upload your latest travel claims data to generate the analysis report.")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Upload Travel Claims Excel File",
        type=["xlsx", "xls"],
        help="Upload the Excel file containing travel claims data"
    )
    
    generate_button = st.button(
        "🚀 Generate Travel Claim Analysis Report",
        type='primary',
        disabled=(uploaded_file is None)   # Button disabled until file is uploaded
    )
    
    if generate_button and uploaded_file is not None:
        with st.spinner("Generating Travel Claim Analysis Report..."):
            try:
                # Pass the uploaded file directly to your function
                report_travel_analysis(uploaded_file)   # ← Important change
                
                # Read the generated output file
                output_path = 'travel_claim_analysis.xlsx'
                
                with open(output_path, 'rb') as f:
                    file_data = f.read()
                
                st.success("✅ Report generated successfully!")
                
                st.download_button(
                    label="📥 Download Travel Claim Analysis Report",
                    data=file_data,
                    file_name=f"Travel_Claim_Analysis_Report_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
            except Exception as e:
                st.error(f"❌ Error generating report: {e}")


