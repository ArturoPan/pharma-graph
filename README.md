# Impiricus Clinical Intelligence Graph

An interactive 3D visualization of the pharma-physician influence network for any US state and year, built entirely on public government APIs — no API keys required.

![Graph visualization showing pharmaceutical companies, physicians, drugs, and conditions as interconnected nodes](reference-pic.png)

---

## What It Does

This tool maps the financial relationships between pharmaceutical companies and physicians in the United States, pulling from three official government data sources:

- **CMS Open Payments** — tracks every payment a pharma company makes to a physician (speaking fees, consulting fees, meals, travel, etc.)
- **NPI Registry** — identifies licensed physicians by state and specialty
- **OpenFDA** — links pharmaceutical companies to their drugs and the conditions those drugs treat

The result is a live, explorable graph where you can see which pharma companies pay the most to physicians in a given state, which physicians receive the most payments, and how drugs connect to medical conditions.

![Detail panel showing pharma node with payment breakdown](reference-pic-two.png)

---

## Getting Started

```bash
git clone https://github.com/your-org/impiricus-pharma-graph
cd impiricus-pharma-graph
./start.sh
```

Open [http://localhost:8000](http://localhost:8000). No configuration, no API keys, no build step.

---

## Features

- **State + Year selector** — query any US state from 2018–2024
- **3D force-directed graph** — nodes sized by payment volume, color-coded by type
- **Click to highlight** — click any node to dim unrelated connections
- **Detail panel** — full payment breakdowns, drugs, and conditions per node
- **Ranked sidebar** — top 15 pharma companies by total payments, with live search
- **Stats bar** — total node count, edge count, and aggregate payments
- **Disk + memory cache** — cold load under 10s, cached load under 200ms

---

## Data Sources

All data is public, sourced directly from US government APIs:

- [CMS Open Payments](https://openpaymentsdata.cms.gov/) — financial relationships between pharma and physicians
- [NPI Registry](https://npiregistry.cms.hhs.gov/) — National Provider Identifier database
- [OpenFDA](https://open.fda.gov/) — FDA drug labeling and manufacturer data
