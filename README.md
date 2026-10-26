# Prateek Sharma — Platform Architecture & Engineering Portfolio

> **Note for Reviewers & Executive Search Teams:**  
> Over the past 6+ years, the vast majority of my production platform engineering, API middleware, and data architecture work has been developed inside private enterprise environments—specifically **Royal Bank of Canada's (RBC)** internal GitHub Enterprise and private client repositories for **GSS Engineering Consultants Ltd.**  
>  
> Due to strict banking security, Data Loss Prevention (DLP), and client IP compliance policies, proprietary source code from those environments cannot be mirrored to public GitHub.  
>  
> **This repository serves as an architectural portfolio and clean-room reference blueprint** detailing how I govern, design, test, and operate multi-provider ingestion platforms and secure semantic retrieval pipelines in production. Clean-room reference implementations of the core orchestration (`run.py`), contract execution (`pipeline.py`), and test isolation (`conftest.py`) patterns are included in this repository.

---

## 1. Enterprise Case Study: Multi-Provider Telemetry & Inventory Ingestion Platform

### The Problem Space
Large-scale infrastructure and operations depend on data spread across dozens of external colocation, facility, and telemetry providers. Each external provider exposes radically different API paradigms, authentication protocols, pagination models, and update frequencies—ranging from modern REST and OData APIs to legacy asynchronous report schedulers, industrial time-series streams, and vendors with no API capability at all (relying on emailed unstructured notices and CSV extracts).

### Architectural Objective & Business Impact
* **Architectural Objective:** Design and operate a unified, extensible Python ingestion middleware platform that aggregates heterogeneous external vendor data into a single normalized relational store (`SQL Server`), registered behind an enterprise API gateway (`Apigee` via OpenAPI specifications), with zero cross-vendor namespace collisions and minimal boilerplate when onboarding new providers.
* **Strategic Business ROI:** Eliminated fragmented manual reporting dependencies across global data centers, significantly accelerated the onboarding velocity for new telemetry vendors through standardized module contracts, and provided executive leadership with unified, real-time operational visibility across both modern and legacy infrastructure providers.

### Repository & Reference Architecture Layout
```text
platform-architecture-portfolio/
├── README.md                       # Architectural case studies & system design overview
├── run.py                          # Reference top-level subprocess orchestrator entry point
├── pipeline.py                     # Reference generic per-vendor contract runner & snapshot deduplicator
├── conftest.py                     # Reference dynamic module loader preventing sys.modules collisions
│
├── common/                         # (Enterprise Reference Layout)
│   ├── database.py                 # Shared DB connection & Kerberos/SPNEGO ticket/ccache management
│   ├── graph.py                    # Shared OAuth2 / Entra ID token & pagination helpers
│   └── env.py                      # Environment loader & required-variable validation
├── provider_sync_rest/             # Pattern A: Synchronous REST + Custom Bearer Auth
│   ├── config.py
│   ├── pipeline.py                 # Generic per-vendor glue (auth -> fetch -> transform -> execute)
│   ├── environmental.py            # High-frequency polling module (every few minutes)
│   ├── device_inventory.py         # Snapshot module using DEDUPE_UNCHANGED_SNAPSHOTS
│   ├── provider-a-schema.yaml      # OpenAPI spec for Apigee gateway registration
│   └── table_scripts.sql           # Shared/normalized DDL
├── provider_async_jobs/            # Pattern B: OAuth2 + Dual-Mode (Sync Trial vs. Async Job Polling)
│   ├── config.py                   # Feature-flagged trial vs. full subscription entitlement
│   ├── reports.py                  # Async job lifecycle: POST schedule -> trigger -> poll -> binary parse
│   ├── smartview.py                # Synchronous real-time environmental client
│   ├── pipeline.py
│   └── provider-b-schema.yaml
├── provider_odata_hierarchy/       # Pattern C: OData v4 + Recursive Hierarchy Flattening
│   ├── config.py
│   ├── odata.py                    # Kerberos session -> @odata.nextLink pagination
│   ├── locations.py                # $expand=AncestralLocationInfo hierarchy flattener & joiner
│   └── pipeline.py
├── provider_industrial_streams/    # Pattern D: Industrial Time-Series (AVEVA PI Web API / AF)
│   ├── config.py
│   ├── piapi.py                    # AF hierarchy crawler & stream reader (SPNEGO auth)
│   ├── pdu_utilization.py          # Rolling 12-week utilization extraction per asset pair
│   └── pipeline.py
└── provider_mailbox_fallback/      # Pattern E: No-API Fallback (EWS + Graph API Scraper)
    ├── ews_client.py               # Read-only Inbox scraper via Kerberos/NTLM
    ├── vendor_routing.py           # Sender-to-provider routing map
    ├── event_parser.py             # Unstructured email body regex/state-machine parser
    ├── sharepoint_client.py        # Downstream calendar/list writer via Graph API
    ├── pipeline.py                 # Connect -> dedup check -> parse -> publish -> audit log
    └── schema.sql                  # State tracking & idempotency tables
```

---

### Engineering Governance & Scaling Patterns
To scale development across multiple integrations without accumulating technical debt or fragmented patterns, these strict architectural contracts were established to standardize engineering workflows, minimize boilerplate, and guarantee predictable failovers.

#### A. Standardized Per-Provider Module Contract
To keep onboarding fast and predictable, `pipeline.py` acts as generic orchestration glue (`authenticate` -> `fetch/generate` -> `transform` -> `cursor.executemany` -> `commit`). It knows nothing vendor-specific. Every endpoint or report module implements a strict contract:

- **`TABLE_NAME`**: Target normalized database table identifier.
- **`PARAMS` / `PARAMS_LIST`**: Single parameter dictionary, or a list of parameter dictionaries for endpoints requiring multiple parameterized passes (e.g., querying `ELECTRICAL` and `MECHANICAL` device connection types separately and merging into a single snapshot).
- **`transform_records(record) -> tuple`**: Pure function mapping a vendor payload dictionary to a normalized database row tuple.
- **`INSERT_QUERY`**: Raw SQL (`MERGE` where idempotent rerun deduplication is required; `INSERT` for append-only activity logs).
- **`expand_record` / `group_records` (Optional Hooks)**: Pre-transform hooks for endpoints that return ungrouped nested arrays or split single logical entities across multiple rows.
- **`DEDUPE_UNCHANGED_SNAPSHOTS` (Optional Flag)**: Set-based snapshot comparison to prevent redundant table growth on full-inventory endpoints.

#### B. Handling Structural Disparities Across Providers
1. **High-Frequency vs. Low-Frequency Polling & Snapshot Deduplication:**  
   Environmental telemetry requires polling every few minutes, whereas physical device and badge inventories change infrequently. Individual pipeline runners support `--modules` flags for independent cron scheduling. Furthermore, for vendor APIs that return a full unpaginated device inventory on every call regardless of changes, modules opt into `DEDUPE_UNCHANGED_SNAPSHOTS`. The runner compares the incoming batch (as a normalized set over target `COLUMNS`) against the most recently stored snapshot and skips database writes when no additions, removals, or circuit renumberings occurred.
2. **Dual-Mode Entitlements (Synchronous Trial vs. Asynchronous Job Polling):**  
   When onboarding commercial providers where trial entitlements expose synchronous REST endpoints (`/environment/v1/listCurrent`) but full enterprise subscriptions require asynchronous report generation, the client architecture isolates both behind a configuration flag:
   - **Sync Mode:** Direct REST fetch -> transform -> insert.
   - **Async Job Mode:** `POST /scheduler` (define report) -> `POST /scheduler/{id}/report` (trigger execution) -> poll `GET /reports/{id}` until `SUCCESS` -> `GET /reports/{id}/file` (binary XLS/CSV/PDF stream parsing).
3. **Recursive Hierarchy Flattening (OData):**  
   Facility hierarchies (`World -> Region -> Building -> Floor -> Room -> Area`) stored in OData systems are traversed using `@odata.nextLink` pagination and `$expand=AncestralLocationInfo`, flattening multi-level ancestral trees in Python and merging normalized rows into target analytics tables.
4. **Asset Framework (AF) Tree Traversal vs. REST Lists:**  
   For industrial time-series historians (AVEVA PI Web API) where metrics are backed by calculated PI Points rather than standard REST tables, discovery walks the AF element hierarchy (`KPIs\Asset_Pairs`), resolves child elements, and queries `/streams/{webId}/value` rather than `/attributes/{webId}/value` (which returns `404` on PI-Point-backed calculated attributes).
5. **Resilient Fallback for Non-API Vendors (Idempotent Mailbox State Machine):**  
   When external providers lack APIs and deliver maintenance notices or data extracts via email to a shared read-only enterprise mailbox:
   - Because the mailbox grant is strictly read-only (cannot mark-as-read or move messages to folders), idempotency is enforced via an audit table (`MailboxProcessedMessages`) keyed on Exchange `Message-ID`.
   - Vendor-specific parsers extract change references (`CHG...`, `CS...`), window timestamps, and lifecycle states (`Active`, `Ended`, `Cancelled`).
   - **State Machine Updates:** Initial notices create downstream calendar/event records and log state in `MailboxChanges`. Subsequent notices referencing the same change ID update the existing record in place and append a dated audit line rather than creating duplicate entries. Unparseable emails are logged as `no_event` for safe retry and inspection.

#### C. Subprocess & Namespace Isolation (`run.py` and `conftest.py`)
Because each vendor directory uses identical module names (`config.py`, `pipeline.py`) for developer ergonomics, importing multiple vendors in a single Python process would collide in `sys.modules`.
- **Runtime Isolation:** `run.py` executes each requested vendor pipeline in an isolated subprocess, while each vendor's `config.py` bootstraps the repository root onto `sys.path` via `__file__` so shared `common.*` modules resolve regardless of the working directory.
- **Test Isolation (`conftest.py`):** The `pytest` suite dynamically loads each vendor's modules in isolation against mocked API payloads, verifying `transform_records`, `expand_record`, and `group_records` contracts without `sys.modules` contamination.

---

## 2. Case Study: Closed-Loop Semantic Search & Local LLM Retrieval Pipeline

### The Problem Space & Privacy Mandate
Engineering consultancies and regulated enterprises manage massive volumes of proprietary project specifications, regulatory filings, and client blueprints. Relying on public, cloud-hosted LLM APIs introduces severe Data Loss Prevention (DLP) risks, intellectual property exposure, and compliance violations.

### Architectural Overview & Governance
Designed and deployed an end-to-end, **closed-loop semantic search and local LLM retrieval pipeline** for corporate engineering documentation (**GSS Engineering Consultants Ltd.**). The architecture prioritizes strict local data sovereignty and risk mitigation over third-party cloud dependencies, combining relational warehousing with high-dimensional vector search.

- **Local Ingestion & Chunking (Python ETL):** Automated ingestion pipelines extract unstructured technical documents, normalize metadata, and chunk content with contextual overlap entirely within the internal network boundary.
- **Self-Hosted Vector Indexing (`Qdrant`):** Document chunks are converted into dense vector embeddings and indexed in a privately hosted **Qdrant** instance with payload metadata filtering, enabling sub-second semantic retrieval without external API calls.
- **Private LLM Summarization & Synthesis:** Retrieved context chunks are routed to a locally hosted LLM inference layer, guaranteeing zero data egress or model training exposure while providing engineers with accurate, citation-backed synthesis of historical project data.
- **Structured Analytics (`Snowflake` & BI):** Non-sensitive document metadata, query telemetry, and structured engineering datasets are synchronized into **Snowflake** to power executive and operational dashboards.

---

## 3. Core Technical & Architectural Competencies

- **Enterprise Governance & Leadership:** Platform Architecture, API Strategy & Gateway Registration, Data Loss Prevention (DLP) & Private AI Design, Vendor Onboarding Standardization, Multi-Tiered System Integration
- **Languages:** Python, SQL (T-SQL, Snowflake, MySQL), C#, Java, Bash/Shell
- **APIs & Integration:** REST, OData, Microsoft Graph API, Exchange Web Services (EWS), AVEVA PI Web API, OpenAPI 3.0, Apigee API Gateway, MQTT
- **Data, AI & Storage:** Local LLM Inference, Qdrant (Vector DB), Snowflake, Microsoft SQL Server (`pymssql`), InfluxDB (Time-Series), Microsoft Dataverse
- **Enterprise Identity & Auth:** Kerberos / SPNEGO (`kinit`, keytabs, credential caches), OAuth2 (Client Credentials), Microsoft Entra ID, NTLM
- **Infrastructure & DevOps:** Linux (Ubuntu, RHEL, Arch), Docker, Proxmox VE, Git / GitHub Enterprise, `pytest`, Grafana, ServiceNow, Microsoft Power Platform