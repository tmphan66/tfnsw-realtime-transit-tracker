# TfNSW Real-Time Transit Tracker
![CI](https://github.com/tmphan66/tfnsw-realtime-transit-tracker/actions/workflows/ci.yml/badge.svg)

A real-time data streaming and analytics platform for Sydney public transit, built with Apache Kafka, Apache Flink, and AWS. Live vehicle positions are decoded, joined with trip-delay data, checked for bus "bunching," and served through an interactive dashboard.

**⚠️ Project status: in active development.** Core streaming pipeline (Kafka → Flink → DynamoDB/S3) and the Streamlit dashboard are functional against live data. Containerization, CI/CD, and cloud deployment are still in progress.

![Dashboard screenshot](assets/dashboard-demo.png)
