<div align="center">

# 🛡️ GuardNet AI

### AI-Driven Network Intrusion Detection System for 5G/6G Core Slices

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![PyTorch](https://img.shields.io/badge/PyTorch-2.0%2B-red.svg)](https://pytorch.org/)
[![PyTorch Geometric](https://img.shields.io/badge/PyG-2.3%2B-orange.svg)](https://pytorch-geometric.readthedocs.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-green.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.25%2B-FF4B4B.svg)](https://streamlit.io/)
[![Kafka](https://img.shields.io/badge/Kafka-3.4%2B-black.svg)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/Vishwaraj-79/GuardNet-AI)](https://github.com/Vishwaraj-79/GuardNet-AI/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/Vishwaraj-79/GuardNet-AI)](https://github.com/Vishwaraj-79/GuardNet-AI/network)

</div>

---

## 📌 Overview

**GuardNet AI** is a production-grade, software-based intrusion detection system specifically designed for **5G/6G network slicing environments**. It leverages **Heterogeneous Graph Neural Networks (HGNNs)** to detect sophisticated cyber threats in real-time, including zero-day attacks, lateral movement between slices, and signaling storms.

> *"Just as no one builds an enterprise IP network without an access control layer, no one should build a private 5G network without GuardNet AI."*

### 🎯 The Problem We Solve

| Challenge | Traditional NIDS | GuardNet AI |
|-----------|------------------|-------------|
| **Network Slicing** | ❌ Doesn't understand slices | ✅ Native slice awareness |
| **Zero-Day Attacks** | ❌ Signature-based | ✅ Behavioral anomaly detection |
| **Lateral Movement** | ❌ Blind to cross-slice attacks | ✅ Graph-based path analysis |
| **Real-Time Detection** | ⚠️ Batch processing | ✅ Sub-100ms inference |
| **Explainability** | ❌ Black box | ✅ Attention visualization |

---

## ✨ Key Features

### 🧠 Advanced AI/ML
- **Heterogeneous Graph Neural Networks (HGT)** for modeling UE ↔ NF ↔ Slice relationships
- **Ensemble Architecture**: HGT + Temporal Autoencoder + LSTM with meta-classifier
- **99% Detection Accuracy** with <1.4% False Positive Rate
- **Explainable AI**: SHAP values and attention weight visualization

### ⚡ Real-Time Streaming
- **Apache Kafka** for high-throughput, low-latency data ingestion
- **Apache Spark** for windowed aggregations and feature engineering
- **Sub-100ms** end-to-end detection latency

### 🎯 5G/6G Native
- Native understanding of **eMBB, URLLC, mMTC, Control, and Management** slices
- Slice-specific feature extraction and anomaly detection
- SLA-aware threat assessment

### 🚀 Production Ready
- **Containerized** microservices with Docker
- **Kubernetes** orchestration with auto-scaling
- **Prometheus + Grafana** monitoring stack
- **MLflow** experiment tracking and model registry

### 📊 Interactive Dashboard
- **Real-time network graph visualization**
- **Live confidence tracking** with moving charts
- **Attack simulation** for DDoS, Lateral Movement, and Signaling Storms
- **Alert management** with severity scoring

---

## 🏗️ Architecture
