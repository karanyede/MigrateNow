# MigrateNow — Enterprise Data Migration Workbench

<p align="center">
  <img src="static/images/MigrateNow_Favicon.png" alt="MigrateNow Logo" width="120" style="border-radius: 12px; margin-bottom: 1rem;">
</p>

MigrateNow is a robust, intuitive web-based workbench designed for high-volume data migrations between **ServiceNow** and **Salesforce**. Built with a Python/Flask backend and a modern glassmorphic frontend, it simplifies cross-platform schema translation, field mapping, and bulk data load operations into a seamless, guided workflow.

---

## 📸 Screenshots

*(Add your screenshots below to make the repository more appealing)*

### Dashboard
<p align="center">
  <img src="static/images/screenshots/dashboard.png" alt="MigrateNow Dashboard" width="800" style="border-radius: 8px;">
</p>

### Saved Connections
<p align="center">
  <img src="static/images/screenshots/connections.png" alt="Saved Connections" width="800" style="border-radius: 8px;">
</p>

### Field Mapping
<p align="center">
  <img src="static/images/screenshots/field_mapping.png" alt="Field Mapping" width="800" style="border-radius: 8px;">
</p>

---

## ⚡ Key Features

- **Multi-Directional**: Seamless data transfer across all four ServiceNow and Salesforce vectors.
- **Auto-Discovery & Mapping**: On-the-fly schema detection, visual UI mapping, and CSV mapping uploads.
- **Smart Diffing**: Automatically detects inserts, updates, and skips while ignoring read-only system fields.
- **Async Execution**: Non-blocking background worker threads handle massive bulk loads.
- **Live Tracking**: Real-time progress and count streams to the UI via Server-Sent Events (SSE).

---

## 🏗️ High-Level Architecture (HLD)

MigrateNow is designed to be highly performant, utilizing parallel operations and bulk endpoints to handle large datasets efficiently.

```mermaid
flowchart TD
    User([Operator]) --> UI[Flask Web App]
    UI --> Meta[Metadata Discovery: Schema & Fields]
    Meta --> Map[Field Mapping & CSV Uploads]
    Map --> Run[Async Migration Thread]

    subgraph Fetch Stage
        Run --> SNFetch[ServiceNow: Parallel CSV Exports / REST]
        Run --> SFFetch[Salesforce: Bulk API 2.0 / REST Query]
    end

    subgraph Diff & Process Stage
        SNFetch --> Diff[Diff Engine: Inserts / Updates / Skips]
        SFFetch --> Diff
        Diff --> Load[Target Bulk Loaders]
    end

    Load --> Progress[SSE Stream: Real-time UI Updates]
```

---

## 🛠️ Tech Stack & Integrations

### Core Technologies
- **Backend**: Python 3, Flask, Multi-threading
- **Frontend**: HTML5, Vanilla JavaScript, CSS (Premium Glassmorphism UI)
- **Real-time Comms**: Server-Sent Events (SSE)

### API Integrations
- **ServiceNow**: 
  - **Discovery**: `sys_db_object` & `sys_dictionary`
  - **Reads**: Parallel Partitioned CSV Exports (`/?CSV`) or Keyset-Paginated REST
  - **Writes**: JSONv2 `insertMultiple` and Table API
- **Salesforce**:
  - **Discovery**: Standard REST `/describe` endpoints
  - **Reads**: Bulk API 2.0 Query jobs
  - **Writes**: Bulk API 2.0 Ingest jobs and SObject Collections

---

## 🚀 Getting Started

### 1. Prerequisites
- **Python 3.8+** installed on your system.

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environments
Create a `.env` file in the root directory:
```env
FLASK_SECRET=your-premium-session-secret-key
FLASK_PORT=5000
LOG_LEVEL=INFO
```

### 4. Run the Workbench
Launch the development server:
```bash
python app.py
```
Open [http://localhost:5000](http://localhost:5000) in your browser!

---

## 📝 License

This project is proprietary. All rights reserved.
