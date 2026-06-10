# Stock Analytics Pipeline

A local, full-stack data engineering pipeline built with **PySpark** and **Delta Lake** that implements a classic **Medallion Architecture** (Bronze $\rightarrow$ Gold layers) to process, clean, and aggregate historical stock market data.

---

##  Architecture Overview

The project processes raw stock data through structured data layers to ensure data integrity, optimized storage, and analytical readiness:

```text
  [ Raw Data Sources ] 
          │
          ▼
  ┌────────────────────────────────────────────────────────┐
│ 1. BRONZE LAYER (extract_bronze.py)                    │
│    • Ingests raw API stock feeds into Delta Lake.      │
│                                                        │  
└────────────────────────────────────────────────────────┘

          ▼
  ┌────────────────────────────────────────────────────────┐
│ 2. GOLD LAYER (bronze_to_gold.py)                      │
│    • Reads the single Bronze source of truth.          │
│    • Executes optimized Spark SQL business logic.      │
│    • Output A: Yearly Market Averages (yearly_avg)     │
│    • Output B: Yearly Price Extremes (close_extremes)  │
└────────────────────────────────────────────────────────┘