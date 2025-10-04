#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions of Data Wrangling for Empatica Data

Created on Mon Jun 23 12:48:28 2025
@author: mengqiao chai
"""

import re
from fastavro import reader
import pickle
import numpy as np
import streamlit as st
import io
import json
import math
import pandas as pd

def extract_start_tstamp(filename):
    '''
    Function that extracts and returns the starting tstamp of a .avro file

    Parameters
    ----------
    filename : string

    Returns
    -------
    the starting tstamp in int.

    '''
    # Assumes last 10 digits before '.avro' are the number
    match = re.search(r'(\d{10})(?=\.avro$)', filename)
    return int(match.group(1)) if match else float('inf')



def reading_avro_files(uploaded_files):
    '''
    Function that reads and save the avro files as python list from uploaded data files

    Parameters
    ----------
    uploaded_files : UploadedFile or a list of UploadedFiles
        The return object from st.file_uploader function.

    Returns
    -------
    raw_datas : python list
        the data list with each element being a dict that contains the raw data.

    '''

    # Get all .avro files
    avro_files = [f for f in uploaded_files if f.name.endswith(".avro")]
    st.info(f"{len(avro_files)} avro files have been uploaded")

    # sort(ascending) the files based on the starting tstamp of the avro file
    sorted_files = sorted(avro_files, key=lambda f: extract_start_tstamp(f.name))

    # read all these avro files and put them into a list
    raw_datas = []

    for f in sorted_files:
        try:
            bio = io.BytesIO(f.getvalue())
            records = list(reader(bio))
            raw_datas.extend(records)
            st.success(f"Read raw data from {f.name}")
        except Exception as e:
            st.error(f"Failed to read {f.name}: {e}")

    return raw_datas



def extract_signal_streamlit(raw_datas, measure: str):
    """
    Extract a certain type of signal (EDA, temperature, BVP) from raw data.

    Parameters
    ----------
    raw_datas : list[dict]
        Raw data list that includes all measures.
    measure : str
        The measure to be extracted, supports ['eda', 'temperature', 'bvp'].

    Returns
    -------
    data_dict : dict
        Includes sampling frequency list, signal values, and unix time stamps.
    """

    fs_combine = []
    data_combine = []
    tstamp_combine = []

    for file_idx, raw_data in enumerate(raw_datas):
        try:
            meas_sampfreq = raw_data['rawData'][measure]['samplingFrequency']
            fs_combine.append(meas_sampfreq)

            meas_data = raw_data['rawData'][measure]['values']
            data_combine.extend(meas_data)

            meas_start_unix = raw_data['rawData'][measure]['timestampStart']
            meas_start_unix_s = meas_start_unix / 1e6
            meas_end_unix_s = meas_start_unix_s + (len(meas_data) - 1) / meas_sampfreq

            if tstamp_combine:  # when list is not empty
                tstamp_diff = round(meas_start_unix_s - tstamp_combine[-1], 3)
                st.info(
                    f"⏱ Time gap between file {file_idx} and file {file_idx+1}: {tstamp_diff} seconds"
                )

            meas_tstamps = list(
                np.linspace(meas_start_unix_s, meas_end_unix_s, num=len(meas_data), endpoint=True)
            )
            tstamp_combine.extend(meas_tstamps)

        except Exception as e:
            st.error(f"❌ Failed to extract {measure} from file {file_idx}: {e}")

    st.success(f"✅ Finished {measure.upper()} extraction from {len(raw_datas)} file(s).")

    data_dict = {
        "fs": fs_combine,
        "samples": data_combine,
        "tstamps": tstamp_combine,
    }

    return data_dict


def _nan_to_none(x):
    """Convert NaN/inf to None so JSON is valid."""
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    if isinstance(x, list):
        return [_nan_to_none(v) for v in x]
    if isinstance(x, dict):
        return {k: _nan_to_none(v) for k, v in x.items()}
    return x

class CompactJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        # force indent for objects but not for arrays
        kwargs['indent'] = 2
        kwargs['separators'] = (',', ': ')
        super().__init__(*args, **kwargs)

    def iterencode(self, o, _one_shot=False):
        # use the default encoder
        for s in super().iterencode(o, _one_shot=_one_shot):
            yield s.replace('\n  [\n    ', ' [') \
                   .replace('\n    ', ' ') \
                   .replace('\n  ]', ']')


def serialize_data_dict(data_dict: dict, fmt: str, base_name: str = "data"):
    """
    Turn your data_dict {'fs': [...], 'samples': [...], 'tstamps': [...]} into a downloadable file.

    Parameters
    ----------
    data_dict : dict
        Your extracted measure data.
    fmt : str
        One of: 'pickle', 'json', 'csv'
    base_name : str
        Filename stem without extension.

    Returns
    -------
    (bytes, filename, mime) : tuple
        - bytes: the file content ready for download
        - filename: suggested filename with extension
        - mime: appropriate MIME type for Streamlit's download_button
    """
    fmt = fmt.lower().strip()

    if fmt == "pickle":
        blob = pickle.dumps(data_dict, protocol=pickle.HIGHEST_PROTOCOL)
        return blob, f"{base_name}.pkl", "application/octet-stream"

    if fmt == "json":
        safe_obj = _nan_to_none(data_dict)
        blob = json.dumps(
        safe_obj,
        ensure_ascii=False,
        cls=CompactJSONEncoder
        ).encode("utf-8")
        return blob, f"{base_name}.json", "application/json"

    if fmt == "csv":
        # Only keep 'tstamps' and 'samples' as columns (omit 'fs')
        df = pd.DataFrame({
            "tstamps": data_dict.get("tstamps", []),
            "samples": data_dict.get("samples", []),
        })
        blob = df.to_csv(index=False).encode("utf-8")
        return blob, f"{base_name}.csv", "text/csv"

    raise ValueError("Unsupported format. Use 'pickle', 'json', or 'csv'.")
    
