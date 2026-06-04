# ManOxCo (Man Oxygen Company) - Case Study

## Company Vision

ManOxCo aims to become the leading company in Spain in the production and distribution of medical oxygen, providing a reliable service while respecting the environment, as well as its customers, employees, suppliers, and shareholders.

## Background

At ManOxCo, a liquid oxygen (LOX) dry-out incident occurred at a hospital served by the company, resulting in the tragic death of a patient.

The root cause of the problem is still unknown. However, it is believed that it may have been caused by an unauthorized maintenance shutdown at the LOX production plant in Madrid, which interrupted distribution and caused a rapid depletion of the storage tanks. Even so, the incident remains under investigation.

As a consequence, the affected hospital was unable to receive liquid oxygen on time.

## Objective

To prevent future oxygen depletion incidents, ManOxCo has decided to hire a company that, through the use of technology and a Big Data platform, can support data analysis and predictive planning in order to effectively manage the next 18 months of LOX production, storage, distribution, and consumption.

This new company process will be called LCS: Liquid Control System.

## Available Data

ManOxCo provides sample real-world datasets and metadata with predefined frequencies for analysis.

1. The `plants_production_export.csv` file contains information on the production, storage, and dispatch of liquid oxygen (LOX) across ManOxCo plants.
2. The `lox_truck.csv` file contains detailed information about the truck fleet used by ManOxCo for the transport and distribution of liquid oxygen (LOX) from production plants to hospitals and other customers.
3. The `lox_delivery_export.csv` file contains the detailed record of liquid oxygen (LOX) deliveries made by ManOxCo to the hospitals served by the company.
4. The `lox_hosp_data.csv` file contains reference information about the hospitals served by ManOxCo, focused on storage capacity, replenishment needs, and commercial conditions associated with the supply of liquid oxygen (LOX).
5. The `lox_hosp_cons_export.csv` file records the daily consumption of liquid oxygen (LOX) by the hospitals served by ManOxCo, enabling analysis of demand and product usage behavior over time and by location.
6. The `plants_data.csv` file contains detailed information about ManOxCo liquid oxygen (LOX) production plants, including operational capacity, efficiency, and cost structure.

## Problem Statement

### The real implementation will involve:

- Production and storage data collected in real time through an IoT box connected to the plants' PLC systems.
- Hospital consumption data transmitted live at a minimum frequency of one reading per minute.

### The selected provider must propose a solution capable of:

- Managing LOX production, storage, distribution, and consumption across multiple locations.
- Planning the next 18 months to ensure operational efficiency.
- Predicting and proactively preventing future LOX dry-out incidents, enabling the immediate execution of corrective actions.
- Suggesting the best time to schedule maintenance shutdowns for each plant.

### Technical Challenges

- Production constraints: each plant has a maximum monthly production and storage limit.
- Recommended maintenance shutdowns every 2 to 3 years: delivery logistics for the area under maintenance will be covered by the other plants.

| Plant | Last Shutdown Date | Maintenance Duration |
| --- | --- | --- |
| Madrid LOX Plant | 01/01/2025 | 22 days |
| Barcelona LOX Plant | 01/08/2023 | 20 days |
| Zaragoza LOX Plant | 01/06/2023 | 15 days |
| Alicante LOX Plant | 01/12/2023 | 12 days |
| Gijon LOX Plant | 01/10/2023 | 8 days |

- Demand fluctuations: hospitals show variable oxygen and LOX consumption patterns.
- Real-time data integration: processing and analyzing high-frequency data streams.
- Logistics and distribution: ensuring efficient supply chain planning to maintain stable LOX availability.

## Technological Approach

Each provider will present a solution proposal using its best technology to address the case presented.

### Expected Deliverables

1. A detailed solution proposal covering data extraction, loading, and processing, predictive analytics, and logistics planning.
2. A comprehensive 18-month plan to optimize operations.
3. A predictive mechanism to anticipate and mitigate possible dry-out incidents.
4. A final presentation demonstrating findings, conclusions, and technical implementation.
5. The investment required to implement the proposed solution and the operating costs associated with running it.
6. A high-level roadmap for the implementation of the proposed solution.

## Addendum

Please note that you, as the provider, have already been selected to execute this project. Accordingly, you must work against the backlog, baseline guidelines, and functional scope defined in this repository's `README.md`, using that document as the primary reference for delivery execution.

## Evaluation Criteria

Solutions will be evaluated based on:

- Technical feasibility and innovation.
- Investment within budget.
- Clarity and effectiveness of the final presentation.

ManOxCo expects state-of-the-art proposals that leverage data-driven strategies to improve operational efficiency and patient safety.
