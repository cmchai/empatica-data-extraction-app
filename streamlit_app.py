
# importing all the packages
import streamlit as st

# Try to import the existing functions (okay if not present yet)
try:
    from src.extract_empatica_data import data_wrangling as dw
except Exception: # pragma: no cover
    dw = None

# create basic elements of the App
st.title('Data Extraction from the Empatica wristband')

st.markdown(
    """
    **Developed by [PhD. Mengqiao Chai](https://cmchai.github.io/website/)**  
    🌐 [Website](https://cmchai.github.io/website/) | 💻 [GitHub](https://github.com/cmchai)
    """,
    unsafe_allow_html=True,
)

st.divider()

# step 1.1: upload raw data files
st.subheader("Please upload Empatica raw data files (.avro)")
uploaded = st.file_uploader(
    "Upload one or more .avro files",
    type=["avro"],
    accept_multiple_files=True,
    help="Drag & drop Empatica .avro exports here."
)

# create cached avro data importing function
@st.cache_data(show_spinner=True)
def reading_avro_cached(uploaded_files):
    # call the orginal function
    return dw.reading_avro_files(uploaded_files)

# step 1.2: read all this avro files
if uploaded:
    raw_datas = reading_avro_cached(uploaded)
    st.write(f"Finished reading {len(raw_datas)} raw data files")

st.divider()

####### Step 2:select a specific measure to extract
st.subheader("Please select a measure that you want to extract the data from")
measure = st.selectbox(
    "Choose a physiological measure",
    options=["eda", "temperature", "bvp"],  # exact labels shown to users
    index=None,
    placeholder="Select one measure ...",
)

if st.button("Data Extraction", disabled=not uploaded):
    data_dict = dw.extract_signal_streamlit(raw_datas, measure)
    if data_dict and (data_dict.get("samples") or data_dict.get("tstamps")):
        st.session_state["data_dict"] = data_dict
        st.session_state["extracted_measure"] = measure

st.divider()


####### Step 3: save the data from a certain measure in a specific format
st.subheader("Save / Download")
data_dict = st.session_state.get("data_dict")

# Let the user choose the format
fmt_label = st.selectbox(
    "Choose a file format",
    ["Pickle (.pkl)", "JSON (.json)", "CSV (.csv)"],
    index=0,
)

# define the user selection and the corresponding internal key
FMT_KEY = { 
    "Pickle (.pkl)": "pickle",
    "JSON (.json)": "json",
    "CSV (.csv)": "csv",
}

fmt = FMT_KEY[fmt_label]

# Suggest a base filename and let user put in their perfered name for the file
default_name = measure
file_stem = st.text_input("File name (without extension)", value=default_name)

# Warn about CSV losing 'fs'
if fmt == "csv":
    st.warning(
        "When saving as CSV, only **time stamps** and **data samples** are saved as two columns. "
        "information regarding **sampling frequencies** will not be included."
    )

####### step 4: allow the user to download the data

# check whether the data is there to save and download in the first place
data_dict = locals().get("data_dict")  # safe lookup
if isinstance(data_dict, dict):
    has_samples = len(data_dict.get("samples", [])) > 0
    has_tstamps = len(data_dict.get("tstamps", [])) > 0
    has_data = has_samples or has_tstamps
else:
    has_data = False

# downloading data
if st.button("Prepare file", disabled=not has_data):
    try:
        blob, filename, mime = dw.serialize_data_dict(locals()["data_dict"], fmt, base_name=file_stem)
        st.success(f"File ready: {filename}")
        st.download_button("⬇️ Download", data=blob, file_name=filename, mime=mime)
    except Exception as e:
        st.error(f"Could not prepare the file: {e}")
else:
    if not has_data:
        st.info("Run the extraction step first so there’s data to save.")

