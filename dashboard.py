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

# UI Placeholders
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
    """
    print(f"[INFO] Connecting to Kafka Broker at {KAFKA_BROKER}...")
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'streamlit-spectrum-v6',
        'auto.offset.reset': 'earliest'
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
    msg = consumer.poll(0.1)
    
    if msg is None:
        continue
    if msg.error():
        print(f"[WARNING] Kafka error: {msg.error()}")
        continue
        
    try:
        payload = json.loads(msg.value().decode('utf-8'))
        
        # Schema validation
        if "frequencies" not in payload:
            continue
            
        frequencies = payload["frequencies"]
        power = payload["power"]
        chunks = payload["total_chunks"]
        max_power = payload.get("max_power_intensity", float(np.max(power)))
        mean_power = payload.get("mean_power_intensity", float(np.mean(power)))
        
        freqs_mhz = np.array(frequencies) / 1e6
        
        df = pd.DataFrame({
            "Power Spectral Density": power
        }, index=freqs_mhz)
        
        # Render updated UI
        with info_placeholder.container():
            st.caption(f"Averaged across {chunks} data chunks | X-Axis centered on 0 Hz DC offset")
            
        with metric_placeholder.container():
            col1, col2 = st.columns(2)
            col1.metric(label="Peak Power Spike", value=f"{max_power:,.2f}")
            col2.metric(label="Floor Mean Power", value=f"{mean_power:,.2f}")
            
        chart_placeholder.line_chart(df, height=500, width='stretch')

    except Exception as e:
        st.error(f"Corrupt frame bypassed: {e}")
        continue
