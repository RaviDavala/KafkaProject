# 🎬 OTT Streaming Analytics Pipeline (Kafka + PySpark + SQL + Delta Live Tables)

## 🚀 Project Overview

This project is a **real-time OTT analytics pipeline** built using:

* **Apache Kafka** (data ingestion)
* **Python** (Kafka Producers)
* **PySpark** (transformations)
* **SQL (DLT + analytical queries)**
* **Databricks Delta Live Tables (DLT)** (pipeline orchestration)

It simulates a **production-grade streaming analytics system**, similar to platforms used by OTT providers, enabling real-time insights into user engagement and content performance.

---

## 🏗️ Architecture & Data Flow (Detailed)

```text
                         ┌──────────────────────────┐
                         │      Kafka Producer      │
                         │ (Dimension, event facts) │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │      Kafka Topic         │
                         │   (Raw Streaming Data)   │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │     Bronze Layer (DLT)   │
                         │--------------------------│
                         │ - Read from Kafka        │
                         │ - Binary → String        │
                         │ - JSON Parsing           │
                         │ - Schema Enforcement     │
                         │ - Minimal Transformation │
                         └────────────┬─────────────┘
                                      │
                ┌─────────────────────┴─────────────────────┐
                │                                           │
                ▼                                           ▼
     ┌──────────────────────────┐              ┌──────────────────────────┐
     │ Silver Layer (Valid Data)│              │ Silver Quarantine Tables │
     │--------------------------│              │--------------------------│
     │ - Data Cleaning          │              │ - Missing content_id     │
     │ - Standardize events     │              │ - Missing user_id        │
     │ - Deduplication          │              │ - Negative watch_time    │
     │ - Late Data Handling     │              │ - Invalid event_time     │
     │   (Watermark)            │              │ - Schema violations      │
     │ - Filter valid records   │              │                          │
     └────────────┬─────────────┘              └────────────┬─────────────┘
                  │                                         │
                  │                                         ▼
                  │                          ┌──────────────────────────┐
                  │                          │ Data Quality Metrics     │
                  │                          │--------------------------│
                  │                          │ - Error counts by type   │
                  │                          │ - Daily invalid records  │
                  │                          │ - Pipeline monitoring    │
                  │                          └──────────────────────────┘
                  │
                  ▼
     ┌──────────────────────────┐
     │ Gold Layer (DLT Tables)  │
     │--------------------------│
     │ Business Aggregations    │
     │ KPI Computations         │
     │ BI-ready datasets        │
     └────────────┬─────────────┘
                  │
     ┌────────────┼─────────────────────────────────────────────┐
     │            │                     │                        │
     ▼            ▼                     ▼                        ▼
┌──────────┐ ┌──────────────┐ ┌────────────────────┐ ┌────────────────────┐
│ Genre    │ │ Device Usage │ │ Daily Engagement   │ │ Content Drop-off   │
│ Perf     │ │ Analytics    │ │ (DAU, Watch Time)  │ │ Analysis           │
└──────────┘ └──────────────┘ └────────────────────┘ └────────────────────┘
```

---

## 🔄 Pipeline Breakdown

### 🔹 Bronze Layer (Raw Ingestion)

* Reads streaming data from Kafka
* Converts binary → string → JSON
* Applies schema
* Stores raw structured data

---

### 🔹 Silver Layer (Clean + Reliable Data)

#### ✅ Valid Data Processing

* Standardizes `event_type` (`play`, `resume`)
* Handles nulls & missing values
* Deduplicates records
* Applies watermark for late data

#### ❌ Quarantine Flow

Invalid records are redirected into quarantine tables:

Examples:

* Missing `content_id`
* Missing `user_id`
* Negative `watch_time`
* Invalid timestamps

👉 This enables:

* Data debugging
* Data quality tracking
* Reprocessing strategies

---

### 🔹 Data Quality Layer

* Tracks error counts by type
* Monitors pipeline health
* Provides daily invalid record insights

---

### 🔹 Gold Layer (Business Logic & KPIs)

Built using **DLT (PySpark + SQL)**

---

## 📊 Gold Tables & Metrics

### 🎯 1. Performance by Genre

* total_watch_time
* total_views
* unique_users
* avg_watch_time_per_user

👉 Measures **engagement per genre**

---

### 📱 2. Device Usage Analytics

* total_watch_time per device
* total_views
* watch_time_percentage

👉 Uses **window functions for contribution %**

---

### 📅 3. Daily User Engagement

* Daily Active Users (DAU)
* Total watch hours
* Avg watch time per user

---

### 📉 4. Content Drop-off Analysis

* total_watch_time
* unique_users
* avg_watch_time
* completion_rate (%)
* dropoff_rate (%)

👉 Completion rate is capped at 100% to handle rewatch scenarios

---

### 🚨 5. Data Quality Metrics

* Missing fields count
* Invalid records by type
* Error trends over time

---

## ⚙️ Technologies Used

| Layer      | Technology              |
| ---------- | ----------------------- |
| Ingestion  | Apache Kafka            |
| Processing | PySpark                 |
| Querying   | SQL                     |
| Pipeline   | Delta Live Tables (DLT) |
| Storage    | Delta Lake              |

---

## 🧠 Key Engineering Concepts

### 🔹 Medallion Architecture

* Bronze → Raw
* Silver → Clean
* Gold → Business

---

### 🔹 Late Data Handling

```python
withWatermark("event_time", "1 hour")
```

---

### 🔹 Event Filtering

```text
play + resume → valid engagement
pause, stop → ignored
```

---

### 🔹 Distributed Processing

* Avoided `.collect()`
* Used window functions
* Used joins instead of driver logic

---

### 🔹 Schema Design

* Fact Table → watch events
* Dimension Table → content metadata

---

## 📈 Business Insights Enabled

* Which genre performs best?
* Which device dominates usage?
* Which content has highest drop-off?
* What is user engagement trend?
* Are there data quality issues?

---

## 🚀 Future Enhancements

* Sessionization (user sessions)
* Real-time trending (last 24h)
* Retention metrics (DAU/MAU)
* Recommendation system inputs
* Dashboard integration (Power BI / Tableau)

---

## 🏁 Conclusion

This project demonstrates:

* Real-time streaming pipeline design
* Data engineering best practices
* Business KPI modeling
* Scalable architecture using DLT

It reflects **industry-level data engineering thinking**, not just implementation.

---

## 🙌 Author

**Ravi Davala**

---

## ⭐ Feedback

Feel free to fork, improve, and build on top of this project!
