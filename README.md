# Nexus-big-data
Masters of integration across systems, data, and people  Nexus thrives where complexity becomes overwhelming. They specialize in interoperability, enterprise ecosystems, and large‑scale platform integration. Whether merging multinational data systems or connecting fragmented digital infrastructures, Nexus turns chaos into harmony. 
# Program Context

ManOxCo is a leading industrial gas company specializing in the production and distribution of liquid oxygen (LOX) for medical use.

In January 2025, a critical dry-out event occurred at a hospital due to:

* Unauthorized maintenance at a production plant
* Distribution interruption in Madrid
* Rapid depletion of storage tanks
* Lack of predictive monitoring

The objective of PI-1 is to design and implement a **data platform capable of preventing future dry-outs** through real-time monitoring, predictive analytics, financial optimization, and AI-driven decision intelligence.

# PI-1 Objective

Design and implement a cloud-simulated industrial data platform that:

* Integrates IT (sales, expenses) and OT (production, storage, transport, consumption) data
* Implements a Medallion Architecture (Bronze, Silver, Gold)
* Supports batch and streaming ingestion
* Enables dimensional modeling and analytics
* Provides dry-out risk prediction
* Includes a GPT-based Decision Intelligence Agent
# Target Architecture Overview

The expected architecture includes:

* Docker-based cloud simulation
* Spark cluster (Master + Workers)
* Batch and streaming pipelines
* Bronze (raw), Silver (cleansed), Gold (analytics) layers
* Fact and dimension modeling
* AI decision layer connected to Gold tables

