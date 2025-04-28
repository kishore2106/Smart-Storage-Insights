#!/bin/bash

# Check if Streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "Streamlit is not installed. Installing requirements..."
    pip install -r requirements.txt
fi

# Start the Streamlit dashboard
echo "Starting SSIP Dashboard..."
streamlit run app.py