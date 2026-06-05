# ManOxCo – Source Data Documentation

## Overview

This repository contains the complete source datasets for the **ManOxCo Industrial Data Platform Case Study**.

The scenario simulates an end-to-end Liquid Oxygen (LOX) supply chain including:

* Production plants
* Transportation fleet
* Hospital customers
* Delivery operations
* Operational telemetry
* Financial transactions

The datasets are divided into two main domains:

* **IT Data** → Master and ERP/CRM data
* **OT Data** → Operational and telemetry data

These datasets are designed to support a full **Medallion Architecture implementation (Bronze → Silver → Gold)**.

---

# Data Domains

## IT Data (Enterprise Systems)

| System | Purpose                         |
| ------ | ------------------------------- |
| CRM    | Customer (Hospital) Master Data |
| ERP    | Truck Cost & Fleet Data         |
| ERP    | Plant Production & Cost Data    |

## OT Data (Operational Systems)

| Source                        | Purpose                              |
| ----------------------------- | ------------------------------------ |
| PLC                           | Plant Production & Storage Telemetry |
| Delivery Execution            | Delivery Transactions                |
| Hospital Sensors (Future IoT) | Hospital Consumption                 |

---

# 1. CRM – Hospital Master

**File:** `lox_hosp_data.csv`
*(Source: CRM System – IT Domain)*

---

## Domain

**IT Data – Customer Master Data (CRM)**

---

## Description

This dataset contains customer master data extracted from the CRM system.
Each record represents one hospital that is a commercial customer of ManOxCo.

The file defines:

* Hospital identification attributes
* Operational LOX storage capacity
* Contractual refill threshold
* Contractual price per ton

Although it includes operational information (tank capacity), this dataset is classified as **IT Data** because it originates from the CRM system and represents commercial/customer master data.

---

## Grain

**One row per hospital (customer account)**

---

## Columns

| Column Name                 | Recommended Data Type           | Description                                         | Nullable | Example         |
| --------------------------- | ------------------------------- | --------------------------------------------------- | -------- | --------------- |
| Hospital City               | STRING                          | City where the hospital is located                  | No       | Madrid          |
| Hostipal Name               | STRING                          | Official hospital name (as stored in CRM)           | No       | Doce de Octubre |
| Storage Max Capacity (Tons) | INTEGER                         | Maximum LOX storage capacity in metric tons         | No       | 10              |
| Min to Refill               | STRING (PERCENT) → DECIMAL(5,2) | Minimum tank percentage threshold triggering refill | No       | 12%             |
| price per ton €             | STRING → DECIMAL(10,2)          | Contractual LOX price per ton in EUR                | No       | 2500,00         |

---

## Data Standardization Recommendations (Bronze → Silver)

### 1️⃣ Column Name Normalization

Raw CRM column names should be standardized to enterprise naming conventions:

| Raw Column                  | Silver Layer Standard     |
| --------------------------- | ------------------------- |
| Hospital City               | hospital_city             |
| Hostipal Name               | hospital_name             |
| Storage Max Capacity (Tons) | storage_max_capacity_tons |
| Min to Refill               | min_refill_threshold_pct  |
| price per ton €             | price_per_ton_eur         |

---

### 2️⃣ Data Type Transformations

**Min to Refill**

* Remove `%`
* Convert to decimal
* Example:

  ```
  12% → 0.12
  ```

**price per ton €**

* Replace comma decimal separator with dot
* Convert to DECIMAL(10,2)
* Example:

  ```
  2500,00 → 2500.00
  ```

---

## Data Quality Observations

1. Column name typo:
   `"Hostipal Name"` should be standardized to `"Hospital Name"` in Silver layer.

2. European decimal formatting detected (comma separator).

3. No explicit hospital identifier provided.
   A surrogate key should be generated in Silver layer:

   ```
   hospital_id (UUID or INT)
   ```

---

## Suggested Primary Key

Since no ID is provided:

**Composite Natural Key:**

```
(Hospital City, Hostipal Name)
```

Recommended in Silver:

```
hospital_id (surrogate key)
```

---

## Business Rules

1. Refill Trigger Condition:

   ```
   current_tank_level ≤ storage_max_capacity_tons × min_refill_threshold_pct
   ```

2. Revenue Calculation:

   ```
   revenue = delivered_tons × price_per_ton_eur
   ```

3. Each hospital must have:

   * Exactly one storage capacity
   * Exactly one refill threshold
   * Exactly one contractual price

---

## Role in the Data Platform (Medallion Architecture)

* **Bronze Layer**

  * Raw ingestion from CRM
  * No transformations applied

* **Silver Layer**

  * Column normalization
  * Type casting
  * Surrogate key generation
  * Data validation

* **Gold Layer**

  * Join with:

    * Delivery events
    * Truck allocation
    * Consumption telemetry (OT)
    * Financial reporting
---

# 2. ERP – Truck Delivery Cost Master

**File:** `lox_truck.csv`
*(Source: ERP System – IT Domain)*

---

## Domain

**IT Data – Logistics Cost & Fleet Master Data (ERP)**

---

## Description

This dataset contains truck master data and delivery cost parameters extracted from the ERP system.

Each record represents one delivery truck assigned to a production plant and defines:

* Maximum transport capacity
* Fixed delivery fee (flat fare)
* Variable delivery fee (% applied to loaded volume)
* Cost breakdown at maximum load

This dataset models the **cost structure of LOX deliveries**.

The total delivery cost is calculated as:

```
Total Delivery Cost = Flat Delivery Fare + Variable Delivery Cost
```

Where:

```
Variable Delivery Cost = (Max Capacity × % to Pay per Ton)
```

---

## Grain

**One row per truck**

---

## Columns

| Column Name                | Recommended Data Type | Description                                       | Nullable | Example |
| -------------------------- | --------------------- | ------------------------------------------------- | -------- | ------- |
| truck name                 | STRING                | Unique truck identifier (ERP reference)           | No       | Chulapo |
| plant name                 | STRING                | Production plant where the truck is based         | No       | Madrid  |
| max capacity in kilo tons  | INTEGER               | Maximum truck capacity in tons                    | No       | 44      |
| flat delivery fare €       | DECIMAL(10,2)         | Fixed cost per delivery                           | No       | 5000    |
| % to pay delivery per tons | STRING → DECIMAL(5,2) | Variable percentage applied to transported volume | No       | 50,00%  |
| capacity delivery fare €   | DECIMAL(10,2)         | Variable cost at maximum capacity                 | No       | 22000   |
| total to pay if max load   | DECIMAL(10,2)         | Total cost if truck is fully loaded               | No       | 27000   |

---

## Data Standardization Recommendations (Bronze → Silver)

### 1️⃣ Column Normalization

| Raw Column                 | Silver Layer Standard             |
| -------------------------- | --------------------------------- |
| truck name                 | truck_name                        |
| plant name                 | plant_name                        |
| max capacity in kilo tons  | max_capacity_tons                 |
| flat delivery fare €       | flat_delivery_fare_eur            |
| % to pay delivery per tons | variable_delivery_pct             |
| capacity delivery fare €   | variable_delivery_cost_at_max_eur |
| total to pay if max load   | total_cost_at_max_load_eur        |

---

### 2️⃣ Data Type Transformations

**% to pay delivery per tons**

* Remove `%`
* Replace comma with dot
* Convert to decimal

  ```
  50,00% → 0.50
  ```

**Currency Fields**

* Ensure decimal format normalization
* Store as DECIMAL(10,2)

---

## Business Logic

### 1️⃣ Delivery Cost Formula (General Case)

For a delivery of `X` tons:

```
Variable Cost = X × loaded_tons × variable_delivery_pct
Total Delivery Cost = flat_delivery_fare_eur + Variable Cost
```

⚠️ The % is intended to apply per ton directly and this must be applied in Silver layer transformation logic.

---

### 2️⃣ Maximum Load Validation

If truck is fully loaded:

```
Total Delivery Cost = total_cost_at_max_load_eur
```

Example:

```
Max Capacity = 44 tons
Flat Fare = 5000
Variable % = 50%
Variable Cost at Max = 22000
Total = 27000
```

---

### 3️⃣ X Load Validation

If truck is partially loaded at X Tons:

```
Variable Cost = X × loaded_tons × variable_delivery_pct
Total Delivery Cost = flat_delivery_fare_eur + Variable Cost
```

Example:

```
Max Capacity = 22 tons
Flat Fare = 5000
Variable % = 50%
Variable Cost at Max = 11000
Total = 16000
```

---

## Data Quality Observations

1. European decimal formatting detected.

2. Column naming inconsistencies.

3. “kilo tons” wording may create confusion — confirm whether unit is:

   * Metric tons
   * Kilotons (unlikely operationally)

4. Derived fields exist:

   * `capacity delivery fare €`
   * `total to pay if max load`

   These should be validated against calculation logic during Silver transformation.

---

## Suggested Primary Key

```text
truck_name
```

If trucks could change plant assignment historically, a surrogate key should be introduced.

---

## Role in the Data Platform (Medallion Architecture)

* **Bronze Layer**

  * Raw ERP ingestion

* **Silver Layer**

  * Type normalization
  * % conversion
  * Cost validation logic
  * Unit standardization

* **Gold Layer**

  * Used for:

    * Delivery profitability analysis
    * Margin calculation per hospital
    * Cost optimization models
    * Route optimization with cost constraints
---

# 3. ERP – Plant Production & Cost Master

**File:** `plants_data.csv`
*(Source: ERP System – IT Domain)*

---

## Domain

**IT Data – Production & Plant Cost Master (ERP)**

---

## Description

This dataset contains master data related to ManOxCo production plants.

Each record represents one LOX production plant and defines:

* Maximum daily production capacity
* Maximum LOX storage capacity
* Operational efficiency metrics
* Variable production costs per ton
* Fixed monthly operational costs

This dataset is foundational for:

* Production planning
* Capacity analysis
* Storage optimization
* Cost modeling
* Margin and profitability analysis

---

## Grain

**One row per production plant**

---

## Columns

| Column Name                         | Recommended Data Type | Description                                          | Nullable | Example |
| ----------------------------------- | --------------------- | ---------------------------------------------------- | -------- | ------- |
| plant name                          | STRING                | Production plant identifier                          | No       | Madrid  |
| max daily production (tons)         | DECIMAL(5,2)          | Maximum daily LOX production capacity in metric tons | No       | 4.1     |
| Max LOX Storage Capacity (tons)     | INTEGER               | Maximum LOX storage capacity at plant                | No       | 60      |
| Plant Average Production Efficiency | STRING → DECIMAL(5,2) | Average production efficiency ratio                  | No       | 74,50%  |
| Plant Average Storage Efficiency    | STRING → DECIMAL(5,2) | Average storage efficiency ratio                     | No       | 99%     |
| Cost Energy € per ton               | DECIMAL(10,2)         | Energy cost per ton produced                         | No       | 125     |
| other cost per ton €                | DECIMAL(10,2)         | Additional operational cost per ton                  | No       | 25      |
| fixed monthly cost                  | DECIMAL(12,2)         | Fixed operational cost per month                     | No       | 45000   |

---

## Data Standardization (Bronze → Silver)

### 1️⃣ Column Normalization

| Raw Column                          | Silver Layer Standard         |
| ----------------------------------- | ----------------------------- |
| plant name                          | plant_name                    |
| max daily production (tons)         | max_daily_production_tons     |
| Max LOX Storage Capacity (tons)     | max_storage_capacity_tons     |
| Plant Average Production Efficiency | avg_production_efficiency_pct |
| Plant Average Storage Efficiency    | avg_storage_efficiency_pct    |
| Cost Energy € per ton               | energy_cost_per_ton_eur       |
| other cost per ton €                | other_cost_per_ton_eur        |
| fixed monthly cost                  | fixed_monthly_cost_eur        |

---

### 2️⃣ Data Type Transformations

**European Decimal Format**

* Replace comma with dot
* Convert to DECIMAL

Example:

```
4,1 → 4.10
74,50% → 0.745
```

**Efficiency Columns**

* Remove `%`
* Convert to decimal ratio:

  ```
  99% → 0.99
  ```

---

## Business Logic

### 1️⃣ Effective Daily Production

```id="vlt2n8"
effective_daily_production = max_daily_production_tons × avg_production_efficiency_pct
```

---

### 2️⃣ Effective Storage Capacity

```id="qj3mzk"
effective_storage_capacity = max_storage_capacity_tons × avg_storage_efficiency_pct
```

---

### 3️⃣ Variable Production Cost Per Ton

```id="kp7a2x"
variable_cost_per_ton = energy_cost_per_ton_eur + other_cost_per_ton_eur
```

---

### 4️⃣ Total Monthly Production Cost

```id="p48mz9"
monthly_variable_cost = produced_tons × variable_cost_per_ton
total_monthly_cost = monthly_variable_cost + fixed_monthly_cost_eur
```

---

## Data Quality Observations

1. European decimal formatting detected.
2. Efficiency stored as percentage string.
3. Units must be clearly standardized to:

   * Metric tons
   * EUR currency
4. No historical tracking — assumes static master data.

---

## Suggested Primary Key

```text id="wz8k2y"
plant_name
```

If plants may evolve historically, introduce:

```
plant_id (surrogate key)
valid_from / valid_to (for SCD Type 2 if needed)
```

---

## Role in the Data Platform (Medallion Architecture)

### Bronze

* Raw ERP ingestion

### Silver

* Type normalization
* % conversion
* Cost derivation columns
* Validation of capacity constraints

### Gold

Used for:

* Production capacity planning
* Storage optimization
* Cost simulation models
* Margin analysis (Plant → Truck → Hospital)
* Financial forecasting
---

# 4. OT – Plant Production Telemetry

**File:** `plants_production_export.csv`
*(Source: PLC System – OT Domain)*

---

## Domain

**OT Data – Plant Operational Telemetry (PLC)**

---

## Description

This dataset contains operational telemetry data extracted from the PLC (Programmable Logic Controller) system of each production plant.

Each record represents a time-stamped operational snapshot including:

* Production volume
* Current storage level
* Delivery events (if any)

Unlike previous ERP/CRM datasets (master data), this dataset is **event-based operational data** and changes continuously.

This is the core dataset for:

* Production monitoring
* Storage tracking
* Delivery detection
* Operational analytics
* Near real-time alerting

---

## Grain

**One record per plant per timestamp**

---

## Columns

| Column Name       | Recommended Data Type | Description                                         | Nullable | Example          |
| ----------------- | --------------------- | --------------------------------------------------- | -------- | ---------------- |
| date              | TIMESTAMP             | Date and time of operational measurement            | No       | 01/01/2024 11:00 |
| plant             | STRING                | Production plant identifier                         | No       | Madrid           |
| production (tons) | DECIMAL(8,4)          | LOX produced during the period                      | No       | 3.0396           |
| stored (tons)     | DECIMAL(10,6)         | Current LOX stored in plant tank                    | No       | 3.009204         |
| delivered (tons)  | DECIMAL(8,2)          | LOX delivered at that timestamp (if event occurred) | Yes      | 44               |

---

## Data Standardization (Bronze → Silver)

### 1️⃣ Column Normalization

| Raw Column        | Silver Layer Standard |
| ----------------- | --------------------- |
| date              | event_timestamp       |
| plant             | plant_name            |
| production (tons) | production_tons       |
| stored (tons)     | stored_tons           |
| delivered (tons)  | delivered_tons        |

---

### 2️⃣ Data Type Transformations

**European Decimal Format**

* Replace comma with dot
* Convert to DECIMAL

Example:

```id="d28szp"
3,0396 → 3.0396
```

**Timestamp**

* Convert to ISO format:

  ```
  01/01/2024 11:00 → 2024-01-01 11:00:00
  ```

---

## Business Logic Interpretation

### 1️⃣ Production Accumulation

Production increases storage:

```id="7x3rjm"
stored_tons(t) = stored_tons(t-1) + production_tons - delivered_tons
```

---

### 2️⃣ Delivery Event Detection

When `delivered_tons` is not null:

* A delivery event occurred
* Storage drops significantly after the event

Example observed:

```id="kx9m4f"
15/01/2024 stored ≈ 44.22
16/01/2024 delivered = 44
16/01/2024 stored ≈ 3.08
```

This indicates:

* Full truck load collected (44 tons)
* Storage reset to residual volume

---

### 3️⃣ Capacity Validation

```id="n2b8gl"
stored_tons ≤ max_storage_capacity_tons
production_tons ≤ max_daily_production_tons
```

Must be validated against ERP plant master data.

---

## Data Quality Observations

1. Delivered column is nullable → indicates event-based behavior.
2. Strong numeric precision → requires DECIMAL, not FLOAT.
3. No plant foreign key → must join with ERP plant master.
4. No explicit batch ID or shift ID → purely time-based tracking.

---

## Suggested Primary Key

```text id="f7q19k"
(event_timestamp, plant_name)
```

---

## Role in the Data Platform (Medallion Architecture)

### Bronze

* Raw PLC ingestion
* No transformations

### Silver

* Timestamp normalization
* Decimal normalization
* Data validation
* Event classification (production vs delivery)

### Gold

Used for:

* Real-time dashboards
* Storage threshold alerts
* Production efficiency KPIs
* Delivery frequency analysis
* Capacity optimization
* Integration with Truck Cost & Revenue models

---

# 🔗 Architectural Importance

This dataset connects the full value chain:

```id="3lgc2m"
Plant Production (OT)
      ↓
Truck Delivery (ERP Cost Model)
      ↓
Hospital Revenue (CRM Pricing)
      ↓
Margin & Profitability (Gold Layer)
```

---

# 5. OT – Delivery Transactions

**File:** `lox_delivery_export.csv`
*(Source: Delivery Execution System – OT/Operational Domain)*

---

## Domain

**OT Data – Delivery Transactions**

---

## Description

This dataset contains delivery transaction records representing LOX refills performed at hospital facilities.

Each record represents one delivery event, including:

* Timestamp
* Truck used
* Hospital served
* Volume delivered
* Delivery cost
* Product revenue
* Total invoice value
* Delivery validation status

This dataset is the **core transactional dataset** of the business model.

It connects:

* Plant production (OT)
* Truck cost structure (ERP)
* Hospital pricing (CRM)

---

## Grain

**One row per delivery event (per hospital, per timestamp)**

---

## Columns

| Column Name       | Recommended Data Type | Description                                     | Nullable | Example          |
| ----------------- | --------------------- | ----------------------------------------------- | -------- | ---------------- |
| Date              | TIMESTAMP             | Delivery event timestamp                        | No       | 01/01/2024 12:00 |
| truck             | STRING                | Truck used for delivery                         | No       | Chulapo          |
| Hospital City     | STRING                | Hospital city                                   | No       | Madrid           |
| Hostipal Name     | STRING                | Hospital name (CRM reference)                   | No       | Doce de Octubre  |
| refill in (tons)  | DECIMAL(6,2)          | Quantity delivered to hospital                  | No       | 8.5              |
| delivery cost     | DECIMAL(10,2)         | Operational delivery cost (from ERP cost logic) | No       | 5250             |
| product tot price | DECIMAL(12,2)         | Revenue generated from delivered volume         | No       | 21887.5          |
| total bill        | DECIMAL(12,2)         | Total invoice amount (delivery + product)       | No       | 27137.5          |

---

## Data Standardization (Bronze → Silver)

### 1️⃣ Column Normalization

| Raw Column        | Silver Layer Standard |
| ----------------- | --------------------- |
| Date              | delivery_timestamp    |
| truck             | truck_name            |
| Hospital City     | hospital_city         |
| Hostipal Name     | hospital_name         |
| refill in (tons)  | delivered_tons        |
| delivery cost     | delivery_cost_eur     |
| product tot price | product_revenue_eur   |
| total bill        | total_invoice_eur     |

---

### 2️⃣ Data Type Transformations

**European Decimal Format**

```text
8,5 → 8.50
21887,5 → 21887.50
```

**Timestamp Issues Observed**
Some records contain malformed timestamps:

```
01/01/2024  13:030:00
```

Must be cleaned during Silver ingestion.

---

## Business Logic

### 1️⃣ Product Revenue Calculation

```text
product_revenue_eur = delivered_tons × price_per_ton_eur (from CRM)
```

---

### 2️⃣ Total Invoice Calculation

```text
total_invoice_eur = delivery_cost_eur + product_revenue_eur
```

Validation example:

```
delivery_cost = 5250
product_revenue = 21887.5
total_invoice = 27137.5 ✓
```

---

### 3️⃣ Margin Calculation (Gold Layer)

```text
gross_margin = product_revenue_eur - production_cost - delivery_cost_eur
```

Where:

```
production_cost = delivered_tons × variable_cost_per_ton (from ERP plant master)
```

---

## Data Quality Observations

1. Timestamp inconsistencies.
2. Decimal formatting issues.
3. Derived columns exist:

   * product total price
   * total bill

These should be recomputed in Gold for auditability.

4. Hospital name typo inherited from CRM.

---

## Suggested Primary Key

```text
(delivery_timestamp, truck_name, hospital_name)
```

Recommended in Silver:

```
delivery_id (surrogate key)
```

---

# 🔗 End-to-End Data Model View

You now have a complete enterprise simulation:

### Master Data (IT)

* CRM – Hospital
* ERP – Trucks
* ERP – Plants

### Operational Data (OT)

* PLC – Production & Storage
* Delivery Events – Transactions

---

# 🎯 Full Value Chain

```
Plant Production (OT)
        ↓
Plant Cost Model (ERP)
        ↓
Truck Cost Structure (ERP)
        ↓
Delivery Transaction (OT)
        ↓
Hospital Pricing (CRM)
        ↓
Invoice & Margin (Gold Layer)
```


---

# 6. OT – Hospital Consumption

**File:** `lox_hosp_cons_export.csv`
**(Source: Hospital Telemetry – Currently Manual Consolidation, Future IoT Integration)*

---

## Domain

**OT Data – Hospital Consumption Telemetry**

---

## Description

This dataset contains hospital-level LOX consumption data.

Currently, the data is manually consolidated.
In the target architecture, this dataset should be ingested automatically via:

* IoT sensors
* Smart tank level meters
* Edge devices
* Secure API ingestion

Each record represents a hospital consumption measurement at a given date.

This dataset is critical for:

* Demand forecasting
* Refill planning
* SLA compliance
* Inventory optimization
* Preventing oxygen stockouts

---

## Grain

Current state:

> Multiple records per hospital per day (sub-daily granularity)

Target state:

> One record per hospital per timestamp (IoT-based real-time ingestion)

---

## Columns

| Column Name        | Recommended Data Type            | Description                              | Nullable | Example         |
| ------------------ | -------------------------------- | ---------------------------------------- | -------- | --------------- |
| Date               | DATE (or TIMESTAMP future state) | Measurement date                         | No       | 01/01/2024      |
| Plant              | STRING                           | Supplying plant                          | No       | Madrid          |
| Hospital City      | STRING                           | Hospital city                            | No       | Madrid          |
| Hospital Name      | STRING                           | Hospital identifier                      | No       | Doce de Octubre |
| Consumption (Tons) | DECIMAL(8,5)                     | LOX consumed during measurement interval | No       | 0.02662         |

---

## Data Standardization (Bronze → Silver)

### 1️⃣ Column Normalization

| Raw Column         | Silver Layer Standard |
| ------------------ | --------------------- |
| Date               | consumption_date      |
| Plant              | plant_name            |
| Hospital City      | hospital_city         |
| Hospital Name      | hospital_name         |
| Consumption (Tons) | consumption_tons      |

---

### 2️⃣ Decimal Conversion

European format detected:

```text
0,02662 → 0.02662
```

Must be converted to DECIMAL(8,5).

---

## Business Logic

### 1️⃣ Daily Consumption Aggregation

If multiple records exist per day:

```text
daily_consumption = SUM(consumption_tons)
```

---

### 2️⃣ Storage Depletion Model

Hospital storage decreases according to:

```text
hospital_storage(t) = hospital_storage(t-1)
                      + delivered_tons
                      - consumption_tons
```

---

### 3️⃣ Refill Trigger Logic

Using CRM threshold:

```text
if hospital_storage ≤ storage_max_capacity × min_refill_threshold_pct
    → trigger delivery
```

This dataset enables predictive refill planning.

---

## Data Quality Observations

1. Multiple rows per hospital per day.

2. No timestamp precision (only date).

3. One suspicious outlier observed:

   ```
   0,00268
   ```

   Significantly lower than surrounding values → potential data anomaly.

4. Manual ingestion → risk of:

   * Missing records
   * Duplicates
   * Delayed reporting

---

## Suggested Primary Key

Current:

```text
(consumption_date, hospital_name, sequence_number)
```

Future (IoT):

```text
(consumption_timestamp, hospital_id)
```

---

# 🔗 Strategic Importance

This dataset enables:

### 📊 Predictive Analytics

* Demand forecasting
* Seasonality modeling
* ICU surge detection

### 🚛 Delivery Optimization

* Dynamic routing
* Reduced emergency deliveries
* Improved truck utilization

### 💰 Financial Optimization

* Margin simulation
* Working capital optimization
* Reduced inventory holding costs

---

# 🏗 Full End-to-End Architecture (Now Complete)

You now have:

### IT (Master & Cost Data)

1. CRM – Hospital Master
2. ERP – Truck Cost Master
3. ERP – Plant Production & Cost

### OT (Operational Data)

4. PLC – Plant Production & Storage
5. Delivery Transactions
6. Hospital Consumption

---

# 🔁 Complete Operational Loop

```text
Plant Production (OT)
        ↓
Plant Storage
        ↓
Truck Delivery (ERP Cost)
        ↓
Hospital Storage
        ↓
Hospital Consumption (OT)
        ↓
Refill Trigger (CRM Threshold)
        ↓
New Delivery
```

This is now a **fully simulated industrial digital twin data platform**.

---

# General end to end Medallion Architecture Mapping

## Bronze

* Raw ingestion
* No transformation
* Schema-on-read

## Silver

* Column normalization
* Data type casting
* Percentage conversion
* Decimal standardization
* Constraint validation
* Surrogate key generation

## Gold

* Fact tables:

  * fact_delivery
  * fact_production
  * fact_consumption
  * fact_storage
  * fact_financial

* Dimensions:

  * dim_date
  * dim_plant
  * dim_truck
  * dim_hospital
  * dim_route
  * dim_cost_center
    
* KPIs:

  * Margin
  * Storage utilization
  * Delivery efficiency
  * Production efficiency
  * Cost per ton
  * Forecasted demand

---

# End-to-End Value Chain

```
Plant Production (OT)
        ↓
Plant Cost Model (ERP)
        ↓
Truck Cost Model (ERP)
        ↓
Delivery Event (OT)
        ↓
Hospital Consumption (OT)
        ↓
Refill Trigger (CRM)
        ↓
Invoice & Margin (Gold Layer)
```

---

# Strategic Use Cases

* Predictive Refill Planning
* Route Optimization
* Margin Optimization
* Cost Simulation
* Production Capacity Planning
* Working Capital Optimization
* AI Agent / RAG Q&A over operational data

---

# Educational Purpose

This dataset supports:

* Data Engineering practices
* Medallion Architecture implementation
* Spark transformations
* Batch & Streaming pipelines
* Financial modeling
* Data Governance
* AI / RAG integration

---

# Final Architecture Status

The ManOxCo dataset represents a fully simulated industrial data platform covering:

* Production
* Logistics
* Commercial
* Operational telemetry
* Financial transactions

It is designed to behave like a real enterprise data ecosystem and is ready for implementation within the lab environment.

---

# Intellectual Property

Copyright (c) 2026
Manuel Alejandro Hernández Giuliani

Licensed under the MIT License.
