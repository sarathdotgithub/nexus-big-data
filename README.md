# Nexus PI-1: ManOxCo Oxygen Reliability Transformation

## About Nexus

Masters of integration across systems, data, and people. Nexus thrives where complexity becomes overwhelming. We specialize in interoperability, enterprise ecosystems, and large-scale platform integration. Whether merging multinational data systems or connecting fragmented digital infrastructures, Nexus turns chaos into harmony.

## Program Context

ManOxCo is a leading industrial gas company specializing in the production and distribution of liquid oxygen (LOX) for medical use. In January 2025, a critical dry-out event occurred at a hospital due to:

- Unauthorized maintenance at a production plant
- Distribution interruption in Madrid
- Rapid depletion of storage tanks
- **Lack of predictive monitoring** ← This is what we're fixing

## PI-1 Objective

Design and implement a **cloud-simulated industrial data platform capable of preventing future LOX dry-outs** through:
- Real-time monitoring and data integration
- Predictive analytics for risk forecasting
- Financial optimization and resource allocation
- AI-driven decision intelligence for executives

## Solution Architecture

### Core Platform Capabilities

**Data Integration (Phase 2)**
- Batch ingestion: Sales & expense data (IT domain)
- Streaming ingestion: Production, distribution, consumption (OT domain)
- 6 ManOxCo data sources + simulated real-time feeds

**Medallion Data Architecture (Phase 3)**
- **Bronze Layer**: Raw, immutable data lake (100% data fidelity)
- **Silver Layer**: Cleansed, standardized, deduplicated (99%+ quality)
- **Gold Layer**: Business-ready analytics (optimized for BI & AI)

**Core Analytics (Phase 4)**
- Hospital tank autonomy forecasting
- Network dry-out risk scoring (0-100)
- Early-warning alert system
- Corrective action recommendations

**AI Decision Copilot (Phase 7)**
- GPT-4 grounded in Gold-layer data
- Cross-domain reasoning (operations, finance, risk)
- Executive-level recommendations
- Decision audit logging

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- 8GB RAM minimum (16GB recommended for Spark cluster)

### Setup

```bash
# Clone and navigate to repository
git clone https://github.com/sarathdotgithub/nexus-big-data.git
cd nexus-big-data

# Run initialization script
chmod +x setup.sh
./setup.sh

# Copy environment template
cp config/.env.example config/.env
# Edit config/.env with your API keys

# Start Spark cluster and Jupyter
docker-compose up -d

# Access services:
# - Spark Master UI: http://localhost:8080
# - Worker 1: http://localhost:8081
# - Worker 2: http://localhost:8082
# - Jupyter: http://localhost:8888
```

## Project Structure

```
nexus-big-data/
├── src/                              # Core platform modules
│   ├── ingestion/                   # Batch & streaming pipelines
│   ├── medallion/                   # Bronze/Silver/Gold transformations
│   ├── analytics/                   # Dry-out prevention analytics
│   ├── ai/                          # Decision copilot
│   ├── governance/                  # Data policies & quality
│   ├── config.py                    # Configuration management
│   ├── spark.py                     # Spark session management
│   └── logging.py                   # Structured logging
│
├── tests/                            # Test suite
│   ├── unit/                        # Unit tests
│   └── integration/                 # Integration tests
│
├── notebooks/                        # Jupyter notebooks
│   ├── 01-exploration/              # EDA
│   ├── 02-ingestion/                # Pipeline development
│   ├── 03-medallion/                # Architecture implementation
│   ├── 04-analytics/                # Analytics development
│   └── 05-ai/                       # Copilot development
│
├── data/                             # Data lake
│   ├── raw/                         # Source CSVs
│   ├── bronze/                      # Raw ingested data
│   ├── silver/                      # Cleansed data
│   └── gold/                        # Analytics data
│
├── docs/                             # Documentation
│   ├── ARCHITECTURE.md              # Technical architecture
│   ├── GOVERNANCE.md                # Data governance
│   └── OPERATIONS.md                # Runbooks
│
├── config/                           # Configuration files
│   └── .env.example                 # Environment template
│
├── docker-compose.yml               # Spark cluster definition
├── setup.sh                         # Initialization script
├── pyproject.toml                   # Dependencies & metadata
└── README.md                        # This file
```

## Technology Stack

- **Orchestration**: Apache Spark 3.5+ (distributed processing)
- **Cluster Simulation**: Docker Compose (1 Master + 2 Workers)
- **Storage**: Local filesystem with Parquet format
- **Languages**: Python 3.10+, PySpark, SQL
- **APIs**: FastAPI (async REST endpoints)
- **AI/ML**: OpenAI GPT-4, LangChain, scikit-learn
- **Data Format**: Parquet (columnar, compression-friendly)
- **Logging**: Loguru (structured logging)
- **Testing**: pytest, pytest-cov

## Implementation Roadmap

### Phase 1: Foundation & Infrastructure ✅
- [x] Project structure
- [x] Docker Spark cluster
- [x] Configuration management
- [x] Logging framework

### Phase 2: Data Integration (In Progress)
- [ ] Batch pipelines (sales, expenses)
- [ ] Streaming pipelines (production, consumption)
- [ ] 6 ManOxCo data source connectors
- [ ] Data validation & profiling

### Phase 3: Medallion Architecture
- [ ] Bronze layer (raw ingestion)
- [ ] Silver layer (cleansing)
- [ ] Gold layer (analytics)
- [ ] Dimensional modeling

### Phase 4: Dry-Out Prevention
- [ ] Autonomy forecasting
- [ ] Risk scoring engine
- [ ] Early warning system
- [ ] Alert notifications

### Phase 5: Planning & Optimization
- [ ] Maintenance scheduling optimizer
- [ ] 18-month scenario planning
- [ ] Logistics optimization

### Phase 6: Financial Analytics
- [ ] Cost-to-serve analysis
- [ ] Margin reporting
- [ ] Platform ROI model

### Phase 7: AI Decision Copilot
- [ ] Gold-layer data grounding
- [ ] GPT integration
- [ ] Cross-domain reasoning
- [ ] Audit logging

### Phase 8: Governance & Quality
- [ ] Data quality monitoring
- [ ] Security controls
- [ ] Compliance tracking

### Phase 9: Documentation & Delivery
- [ ] Technical proposal
- [ ] Operating plan
- [ ] Executive presentation
- [ ] Implementation roadmap

## Key Concepts

### Medallion Architecture

Our three-layer data architecture ensures data quality and business value:

1. **Bronze (Raw)**: Immutable record of all ingested data with source metadata
2. **Silver (Cleansed)**: Standardized, validated, deduplicated data ready for analysis
3. **Gold (Analytics)**: Star schema with facts, dimensions, and pre-aggregated metrics

### Dry-Out Prevention Model

The platform predicts and prevents oxygen shortages through:
- **Autonomy Calculation**: Hours of tank supply remaining at current consumption
- **Demand Forecasting**: Historical patterns + seasonal trends
- **Risk Scoring**: 0-100 scale (LOW → MEDIUM → HIGH → CRITICAL)
- **Corrective Actions**: Reroute, expedite, rebalance, defer maintenance

### AI Decision Intelligence

The GPT-based copilot:
- Grounds responses in Gold-layer data (not hallucinations)
- Reasons across operations, finance, and risk domains
- Provides executive-level recommendations with justification
- Maintains audit trail of all decisions

## Governance & Quality

**Data Quality SLAs**
- 99%+ completeness across layers
- 24-hour data freshness for batch
- Real-time ingestion for critical OT data
- Automated anomaly detection

**Definition of Done**
Every feature requires:
- Code + tests + documentation
- Data validation checks
- Architecture review
- PR approval + CI/CD passing
- No high/critical security vulnerabilities

## Success Metrics

| Metric | Target |
|--------|--------|
| Dry-out prediction accuracy | >95% |
| Data completeness | 99%+ |
| Platform uptime | 99.5% |
| Analytics query response (p95) | <5 seconds |
| AI agent response (p95) | <10 seconds |
| Operational efficiency improvement | +20% |
| Margin improvement | +15% |

## Documentation

- **Architecture**: See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Governance**: See [docs/GOVERNANCE.md](docs/governance/__init__.py)
- **Operations**: See [docs/OPERATIONS.md](docs/OPERATIONS.md)

## Contributing

1. Ensure changes align with governance policies
2. Write tests for all new features
3. Update documentation
4. Pass code review
5. Merge to main

## License

MIT License - Copyright (c) 2026 Nexus Consulting

---

**Welcome to PI-1. Let's prevent oxygen dry-outs and transform industrial operations.**

