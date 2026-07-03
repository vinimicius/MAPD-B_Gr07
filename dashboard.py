import json
import sys
import pandas as pd
import streamlit as st
import numpy as np
from confluent_kafka import Consumer

# ==========================================
# 1. PAGE CONFIGURATION & UI INIT
# ==========================================
st.set_page_config(page_title="Live FFT Spectrum", page_icon="📡", layout="wide")

st.title("📡 Centered FFT Power Spectral Density")
st.markdown("Streaming live from the bare-metal Dask cluster via Apache Kafka.")

# --- UI Placeholders ---
# These prevent the page from flickering or resetting when new data arrives
info_placeholder = st.empty()
metric_placeholder = st.empty()
chart_placeholder = st.empty()


# ==========================================
# 2. KAFKA CONFIGURATION
# ==========================================
KAFKA_BROKER = '10.67.22.134:9092'             # VM2
TOPIC = 'topic_results'                        # Outgoing stream from VM3

@st.cache_resource
def get_kafka_consumer():
    """
    Creates and caches the Kafka consumer.
    Using @st.cache_resource ensures Streamlit doesn't recreate the connection
    every time the script reruns.
    """
    print(f"[INFO] Connecting to Kafka Broker at {KAFKA_BROKER}...")
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'streamlit-spectrum-v6',   # Bumped to v6 for a clean memory slate
        'auto.offset.reset': 'earliest'        # Read the data that is already waiting!
    })

try:
    consumer = get_kafka_consumer()
    consumer.subscribe([TOPIC])
except Exception as e:
    st.error(f"Failed to connect to Kafka Broker: {e}")
    sys.exit(1)


# ==========================================
# 3. LIVE STREAMING REACTION LOOP
# ==========================================
while True:
    # Poll Kafka for a message (100ms timeout)
    msg = consumer.poll(0.1)
    
    if msg is None:
        continue
    if msg.error():
        print(f"[WARNING] Kafka error: {msg.error()}")
        continue
        
    try:
        # Decode binary payload to dictionary
        payload = json.loads(msg.value().decode('utf-8'))
        
        # --- DATA SCHEMA VALIDATION ---
        # If an old summary message is encountered, skip it safely
        if "frequencies" not in payload:
            continue
            
        # Extract matrices and metadata sent by VM3
        frequencies = payload["frequencies"]
        power = payload["power"]
        chunks = payload["total_chunks"]
        max_power = payload.get("max_power_intensity", float(np.max(power)))
        mean_power = payload.get("mean_power_intensity", float(np.mean(power)))
        
        # --- DATA PREPARATION ---
        # Convert raw frequencies (Hz) to Megahertz (MHz) for clean visualization labels
        freqs_mhz = np.array(frequencies) / 1e6
        
        # Wrap into a Pandas DataFrame using the frequencies directly as the indexing row
        df = pd.DataFrame({
            "Power Spectral Density": power
        }, index=freqs_mhz)
        
        # --- 4. RENDER UPDATED WEB ELEMENT CONTENT ---
        # Update text caption
        with info_placeholder.container():
            st.caption(f"Averaged across {chunks} data chunks | X-Axis centered on 0 Hz DC offset")
            
        # Update metric blocks
        with metric_placeholder.container():
            col1, col2 = st.columns(2)
            col1.metric(label="Peak Power Spike", value=f"{max_power:,.2f}")
            col2.metric(label="Floor Mean Power", value=f"{mean_power:,.2f}")
            
        # Draw the physical zero-centered line spectrum chart
        chart_placeholder.line_chart(df, height=500, width='stretch')

    except Exception as e:
        # Display unexpected structural or JSON decoding errors directly on the UI
        st.error(f"Corrupt frame bypassed: {e}")
        continue
