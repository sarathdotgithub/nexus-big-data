# PI-1 Platform Architecture Documentation

## Overview

The ManOxCo Oxygen Reliability Transformation (PI-1) platform is a cloud-simulated industrial data platform designed to prevent liquid oxygen (LOX) dry-out events in hospitals through real-time monitoring, predictive analytics, and AI-driven decision intelligence.

## Platform Objectives

1. **Prevent Oxygen Dry-Outs**: Real-time monitoring and predictive analytics to anticipate and prevent LOX depletion incidents
2. **Data Integration**: Unified ingestion of IT (sales, expenses) and OT (production, storage, distribution, consumption) data
3. **Operational Optimization**: 18-month planning and maintenance scheduling to ensure optimal resource utilization
4. **Financial Analysis**: Cost-to-serve and margin optimization across distribution networks
5. **Decision Intelligence**: GPT-based decision copilot providing executive-level insights and recommendations

## Architecture Layers

### 1. Ingestion Layer
- **Batch Ingestion**: Sales and expense data (daily/weekly)
- **Streaming Ingestion**: Plant production and hospital consumption data (real-time)
- **Data Sources**: 6 ManOxCo datasets + simulated live feeds

### 2. Storage & Processing Tiers

#### Bronze Layer (Raw Data)
- Immutable, append-only storage of raw ingested data
- Maintains data lineage and ingestion metadata
- Serves as single source of truth for raw data

#### Silver Layer (Cleansed Data)
- Standardized data format and naming conventions
- Data quality validation and anomaly detection
- Harmonized business keys and dimensions
- Deduplicated and normalized records

#### Gold Layer (Analytics)
- Dimensional modeling with facts and dimensions
- Business-ready analytical marts
- Pre-aggregated metrics for operational, financial, and risk analytics
- Optimized for query performance and AI reasoning

### 3. Analytical Domains

#### Operational Analytics
- Hospital tank autonomy and consumption forecasting
- Plant production and storage capacity utilization
- Distribution network optimization
- Truck fleet logistics and route planning

#### Risk Analytics
- Dry-out risk scoring and early warning signals
- Supply-demand balance monitoring
- Constraint violation detection
- Maintenance window optimization

#### Financial Analytics
- Cost-to-serve by route, hospital, and plant
- Margin analysis and profitability reporting
- Operational efficiency metrics
- Capital and operating cost modeling

### 4. AI Decision Intelligence Layer
- GPT-based conversational agent
- Grounded in Gold-layer analytical tables
- Cross-domain reasoning (operations, finance, risk)
- Executive-level recommendations
- Audit logging and decision traceability

## Data Flow

```
Source Data (Batch & Streaming)
         ↓
    Ingestion Layer
         ↓
    Bronze Layer (Raw)
         ↓
    Silver Layer (Cleansed)
         ↓
    Gold Layer (Analytics)
         ↓
    Analytical Applications & AI Agent
```

## Technology Stack

- **Orchestration**: Apache Spark 3.5+
- **Cluster**: Master + 2+ Worker nodes (Docker-based simulation)
- **Storage**: Local filesystem (Bronze/Silver/Gold directories)
- **Data Format**: Parquet (columnar, efficient storage)
- **API**: FastAPI with async support
- **AI**: OpenAI GPT-4 integration via LangChain
- **Monitoring**: Custom logging and data quality checks

## Project Structure

```
nexus-big-data/
├── src/
│   ├── ingestion/          # Data ingestion pipelines (batch & streaming)
│   ├── medallion/          # Medallion architecture transformations
│   ├── analytics/          # Analytical mart creation
│   ├── ai/                 # AI decision copilot
│   ├── governance/         # Data governance and quality controls
│   ├── config.py           # Configuration management
│   ├── spark.py            # Spark session management
│   └── logging.py          # Logging configuration
├── tests/
│   ├── unit/               # Unit tests
│   └── integration/        # Integration tests
├── notebooks/
│   ├── 01-exploration/     # EDA and data exploration
│   ├── 02-ingestion/       # Ingestion pipeline development
│   ├── 03-medallion/       # Medallion architecture development
│   ├── 04-analytics/       # Analytics development
│   └── 05-ai/              # AI agent development
├── docs/
│   ├── architecture/       # Architecture documentation
│   ├── governance/         # Governance policies
│   └── operations/         # Operational runbooks
├── config/
│   └── .env.example        # Configuration template
├── data/
│   ├── raw/                # Raw source data (ManOxCo CSVs)
│   ├── bronze/             # Raw ingested data
│   ├── silver/             # Cleansed and standardized data
│   └── gold/               # Analytics-ready data
├── docker-compose.yml      # Spark cluster definition
├── setup.sh                # Platform initialization script
└── pyproject.toml          # Project dependencies and metadata
```

## Implementation Phases

### Phase 1: Foundation & Infrastructure ✓
- [x] Project structure
- [x] Docker Spark cluster setup
- [x] Configuration management
- [x] Logging framework

### Phase 2: Data Integration Layer (In Progress)
- [ ] Batch ingestion for IT data (sales, expenses)
- [ ] Streaming ingestion for OT data (production, consumption)
- [ ] Data validation and profiling
- [ ] Error handling and recovery

### Phase 3: Medallion Architecture
- [ ] Bronze layer ingestion
- [ ] Silver layer transformations
- [ ] Gold layer analytical marts
- [ ] Dimensional modeling

### Phase 4: Dry-Out Prevention Capability
- [ ] Hospital autonomy forecasting
- [ ] Risk scoring engine
- [ ] Early warning system
- [ ] Corrective action recommendations

### Phase 5: Planning & Optimization
- [ ] Maintenance window optimizer
- [ ] 18-month scenario planning
- [ ] Logistics optimization

### Phase 6: Financial Analytics
- [ ] Cost-to-serve analysis
- [ ] Margin reporting
- [ ] Platform ROI model

### Phase 7: AI Decision Copilot
- [ ] Data model for AI queries
- [ ] GPT agent integration
- [ ] Cross-domain reasoning

### Phase 8: Governance & Quality
- [ ] Data governance policies
- [ ] Quality monitoring
- [ ] Security and compliance

### Phase 9: Documentation & Delivery
- [ ] Technical proposal
- [ ] Operating plan
- [ ] Executive presentation
- [ ] Implementation roadmap

## Success Metrics

- **Dry-Out Prevention**: Zero predicted dry-out incidents with >95% forecast accuracy
- **Data Quality**: 99%+ data completeness and accuracy across all layers
- **System Availability**: 99.5% uptime for analytics platform
- **Decision Velocity**: <5 second response time for AI agent queries
- **Operational Efficiency**: 20%+ improvement in logistics optimization
- **Financial Impact**: 15%+ margin improvement through cost optimization

## Next Steps

1. Initialize data lake directories and load sample datasets
2. Develop ingestion pipelines for batch and streaming data
3. Create Bronze/Silver/Gold transformations
4. Implement dry-out prediction models
5. Deploy AI decision copilot
6. Execute final validation and documentation
