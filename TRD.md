
## Technical Requirements Document (TRD)

### 1. System Architecture
The system utilizes a **Hub-and-Spoke** architecture. A central Python engine (The Hub) reads configuration and dispatches validation tasks to remote MySQL databases (The Spokes) in parallel.

#### 1.1 Technology Stack
* **Core Engine:** Python 3.9+
* **Validation Framework:** Great Expectations (GX) v1.0+
    * *Why:* Provides standard statistical tests and robust "Unexpected Rows" handling out of the box.
* **Concurrency:** `concurrent.futures.ThreadPoolExecutor`
    * *Why:* Efficient handling of I/O bound database queries.
* **Interface:** Streamlit
    * *Why:* Rapid UI development for internal tools.
* **Database Connectivity:** `SQLAlchemy` + `mysql-connector-python`

### 2. Module Design

#### 2.1 Configuration Layer
* **`config/gx_rules.yaml`**: The "Brain". Stores the Expectation Suite definition.
* **`secrets.toml`**: The "Vault". Stores DB hosts, users, passwords, and SMTP credentials.
* **`logging.conf`**: Defines log rotation and formatting.

#### 2.2 Execution Engine (`src/gx_wrapper.py`)
This class encapsulates the Great Expectations complexity.
* **Method `run_validation(lender_id)`**:
    1.  Creates an **Ephemeral Data Context**.
    2.  Builds a dynamic connection string for the requested `lender_id`.
    3.  Translates `gx_rules.yaml` into a GX `ExpectationSuite`.
    4.  Executes a Checkpoint against the database.
    5.  Parses the complex GX Result Object into a flat Pandas DataFrame.

#### 2.3 Notification System (`src/notifier.py`)
* Accepts a DataFrame of failed tests.
* Generates an HTML body with a CSS-styled table.
* Sends via SMTP (Office365/Gmail/SendGrid).
* **Logic:** If `DataFrame.empty` is True, no email is sent.

### 3. Application Flow Diagram

#### 3.1 Automated Daily Flow (Headless)
1.  **Trigger:** Windows Task Scheduler executes `daily_job.py`.
2.  **Init:** Script loads `secrets.toml` and `gx_rules.yaml`.
3.  **Fan-Out:** Script spawns Worker Threads.
4.  **Execute:** Each thread claims a Lender, connects to MySQL, runs SQL checks.
5.  **Fan-In:** Results from all threads are merged into a Master DataFrame.
6.  **Decision:**
    * If `Failures == 0`: Log "Success" and exit.
    * If `Failures > 0`: Call `notifier.send_alert_email()` -> Exit.

#### 3.2 Manual User Flow (UI)
1.  **Trigger:** User opens Streamlit App in browser.
2.  **Selection:** User selects "Lender A" from sidebar.
3.  **Action:** User clicks "Run Diagnostics".
4.  **Execute:** `GXRunner` runs validation for *only* Lender A (Single Thread).
5.  **Display:** Results are rendered in a colored Data Grid on screen. Email is **not** sent (default behavior for UI, to avoid spam).

### 4. Data Model (Output)
The system standardizes results into a flat format for easy reporting:

| Field | Type | Description |
| :--- | :--- | :--- |
| `lender` | String | The identifier (e.g., "lender_a") |
| `test_name` | String | The GX Expectation Type (e.g., "expect_column_values_to_not_be_null") |
| `status` | String | "PASS", "FAIL", or "CRITICAL_ERROR" |
| `failed_rows` | Integer | Count of rows violating the rule |
| `severity` | String | "critical" or "warning" (defined in YAML) |
| `error_msg` | String | Stack trace if status is ERROR |

### 5. Deployment Structure
The application requires the following directory structure on the Windows Application Server:

```text
C:\Apps\LenderDQ\
│
├── config/
│   ├── gx_rules.yaml      # The "Test Definitions" (Edit this to add rules)
│   └── logging.conf       # Log rotation settings
│
├── src/
│   ├── __init__.py        # (Empty file)
│   ├── gx_wrapper.py      # The "Brain": Runs GX in parallel
│   ├── notifier.py        # The Emailer: Sends HTML alerts
│   └── app.py             # The UI: Streamlit Dashboard
│
├── logs/                  # (Auto-created) Stores daily log files
├── daily_job.py           # The script for Windows Task Scheduler
├── secrets.toml           # Database & Email Credentials (DO NOT COMMIT TO GIT)
└── requirements.txt       # Python dependencies
```