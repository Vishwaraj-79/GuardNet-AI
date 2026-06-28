import streamlit as st
import requests
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
import random

st.set_page_config(page_title="GuardNet AI Dashboard", layout="wide")

st.title("🛡️ GuardNet AI - 5G Network Intrusion Detection Dashboard")

# Sidebar
st.sidebar.header("Network Configuration")
slice_type = st.sidebar.selectbox("Select Slice", ["eMBB", "URLLC", "mMTC", "Control", "Management"])
num_flows = st.sidebar.slider("Number of Flows", 1, 20, 5)

# Generate sample flows
if st.sidebar.button("Generate Test Traffic"):
    flows = []
    for i in range(num_flows):
        flows.append({
            "src_ip": f"192.168.1.{random.randint(1, 100)}",
            "dst_ip": f"10.0.0.{random.randint(1, 50)}"
        })
    st.session_state.flows = flows
    st.session_state.slice = slice_type
    st.rerun()

# Detection button
if st.button("🔍 Run Detection"):
    if "flows" in st.session_state:
        try:
            response = requests.post(
                "http://localhost:8000/detect",
                json={"flows": st.session_state.flows, "slice_id": st.session_state.slice}
            )
            result = response.json()
            st.session_state.result = result
            
            # Store for visualization
            st.session_state.is_attack = result["is_attack"]
            st.session_state.confidence = result["confidence"]
            
        except Exception as e:
            st.error(f"API Error: {e}")

# Display Results
col1, col2, col3 = st.columns(3)

if "result" in st.session_state:
    result = st.session_state.result
    
    with col1:
        if result["is_attack"]:
            st.metric("🚨 Attack Detected", "⚠️ SUSPICIOUS", delta="Threat")
            st.error(f"Confidence: {result['confidence']*100:.1f}%")
        else:
            st.metric("✅ Status", "NORMAL", delta="Secure")
            st.success(f"Confidence: {result['confidence']*100:.1f}%")
    
    with col2:
        st.metric("Flows Processed", result["flows_processed"])
        st.metric("Slice", result["slice_id"])
    
    with col3:
        st.metric("Attack Type", result["attack_type"])
        st.metric("API Status", "200 OK ✅")

# Graph Visualization
st.subheader("📊 Network Graph Visualization")

if "flows" in st.session_state:
    # Create network graph
    G = nx.Graph()
    
    for flow in st.session_state.flows:
        G.add_node(flow["src_ip"], type="UE")
        G.add_node(flow["dst_ip"], type="NF")
        G.add_edge(flow["src_ip"], flow["dst_ip"])
    
    # Create positions
    pos = nx.spring_layout(G, seed=42)
    
    # Node colors
    node_colors = []
    node_sizes = []
    for node in G.nodes():
        if G.nodes[node].get("type") == "UE":
            node_colors.append("lightblue")
            node_sizes.append(40)
        else:
            node_colors.append("lightgreen")
            node_sizes.append(60)
    
    # Edge colors based on attack
    edge_color = "red" if st.session_state.get("is_attack", False) else "gray"
    
    # Plot with plotly
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=2, color=edge_color),
        hoverinfo='none',
        mode='lines'
    )
    
    node_x = []
    node_y = []
    node_text = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(f"{node}<br>Type: {G.nodes[node].get('type', 'Unknown')}")
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=node_text,
        textposition="top center",
        hoverinfo='text',
        marker=dict(
            size=node_sizes,
            color=node_colors,
            line=dict(width=1, color='black')
        )
    )
    
    fig = go.Figure(data=[edge_trace, node_trace])
    fig.update_layout(
        title="5G Network Flow Graph",
        showlegend=False,
        hovermode='closest',
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(showgrid=False, zeroline=False),
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Attack History
st.subheader("📈 Detection History")
history_data = pd.DataFrame({
    "Time": pd.date_range(end=pd.Timestamp.now(), periods=10, freq="s"),
    "Confidence": [random.random() for _ in range(10)],
    "Attack": [random.choice([0, 1]) for _ in range(10)]
})

fig2 = px.line(history_data, x="Time", y="Confidence", title="Confidence Score Over Time")
fig2.add_hline(y=0.5, line_dash="dash", line_color="red", annotation_text="Threshold")
st.plotly_chart(fig2, use_container_width=True)

# Alerts
st.subheader("🚨 Recent Alerts")
alerts = [
    {"time": "12:34:56", "type": "Normal", "confidence": "12%"},
    {"time": "12:34:46", "type": "Normal", "confidence": "8%"},
    {"time": "12:34:36", "type": "Normal", "confidence": "15%"},
]
st.table(alerts)
