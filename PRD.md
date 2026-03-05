## Product Requirements Document (PRD)

### 1. Executive Summary
We are building a production-grade Data Quality (DQ) framework to validate data integrity across multiple distinct lender MySQL databases. As the organization scales, manual validation of these Data Warehouses (DWH) is no longer sustainable. This system will automate daily checks, provide a manual trigger for ad-hoc validation after data syncs, and alert both Data Engineering and Analytics teams of critical failures.

### 2. Problem Statement
* **Fragmentation:** Data is siloed across multiple lender databases with identical schemas but distinct connections.
* **Manual Effort:** Validation currently relies on ad-hoc SQL queries run manually via Workbench/Python.
* **Risk:** Schema changes or ETL failures may go unnoticed until a report breaks.
* **Visibility:** No centralized view of data health across all lenders.

### 3. User Personas
* **Data Analyst (DA):** Defines business rules (e.g., "Loan Amount cannot be NULL"). Needs to run manual checks after modifying logic or resyncing data.
* **Data Engineer (DE):** Maintains the pipeline. Needs to be notified immediately if connection issues or critical schema violations occur.

### 4. Functional Requirements (FR)

| ID | Requirement | Description | 
| :--- | :--- | :--- 
| **FR-01** | **Multi-Tenant Support** | System must connect to any of the lender databases dynamically based on configuration.
| **FR-02** | **Configurable Rules** | Validation rules must be defined in a human-readable config file (YAML), not hardcoded in Python.
| **FR-03** | **Automated Surveillance** | A scheduled job must run all tests across all lenders daily (e.g., 6:00 AM) without human intervention.
| **FR-04** | **Manual UI Trigger** | A simple Dashboard (Streamlit) must allow users to trigger validations for a specific lender on demand.
| **FR-05** | **Alerting** | Critical failures and daily summaries must be emailed to teams as a high-fidelity HTML report with Streamlit-like styling.
| **FR-06** | **Parallel Execution** | Tests should run concurrently using multi-processing to ensure thread-safety and performance.
| **FR-07** | **Detailed Reporting** | In addition to HTML summaries, the system must generate downloadable CSVs containing ALL failed rows for root cause analysis.

### 5. Non-Functional Requirements (NFR)
* **Scalability:** Must handle more lenders without code refactoring.
* **Security:** Database and SMTP credentials must be isolated via `secrets.toml`, with support for Environment Variables for sensitive passwords.
* **Auditability:** All runs (manual and automated) must produce a log file and a timestamped HTML summary.
* **Maintainability:** Built on standard libraries (Great Expectations, Pandas) and modular design to ensure longevity.
