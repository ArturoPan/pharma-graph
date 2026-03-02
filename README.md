# Impiricus Clinical Intelligence Graph

An interactive 3D map of the pharma-physician influence network in the US — explore which companies pay doctors, how much, and for which drugs.

## 🌐 Live Site

**[imp-pharma-graph.vercel.app](https://imp-pharma-graph.vercel.app)**

---

## 💡 Why I Built This

Every year, pharmaceutical companies transfer billions of dollars to physicians in the form of speaking fees, consulting contracts, meals, and travel — all legally required to be reported to the federal government. But the raw data sits in dense government databases that are nearly impossible to explore. I built this tool to make those financial relationships visible: who is paying whom, how much, and which drugs and medical conditions are at the center of it.

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | Python + FastAPI |
| Graph engine | NetworkX |
| 3D visualization | Three.js + 3d-force-graph |
| Styling | Vanilla CSS |
| Data pipeline | CMS Open Payments, NPI Registry, OpenFDA |
| Hosting | Vercel (static) |

---

## 🗂 Data Sources

All data is public — sourced directly from US government APIs, no API keys required.

- **[CMS Open Payments](https://openpaymentsdata.cms.gov/)** — every payment a pharma company makes to a physician (consulting fees, speaker honoraria, travel, meals, grants)
- **[NPI Registry](https://npiregistry.cms.hhs.gov/)** — licensed physicians by state and specialty
- **[OpenFDA](https://open.fda.gov/)** — FDA drug labels linking companies to their drugs and the conditions they treat

---

## 🚀 Run Locally

**Prerequisites:** Python 3.12+

1. Clone the repo and start the server:
   ```bash
   git clone https://github.com/ArturoPan/pharma-graph
   cd pharma-graph
   ./start.sh
   ```
2. Open [http://localhost:8000](http://localhost:8000) — no configuration, no API keys, no build step.

To pre-fetch data for additional states/years:
```bash
.venv/bin/python -m scripts.prefetch          # fetch all default targets
.venv/bin/python -m scripts.prefetch TX 2024  # fetch a single state/year
```
