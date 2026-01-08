# Project Guide: The "Plain English" Explanation

This document breaks down every component of the Data Quality Framework using real-world analogies.

---

### 1. The "Black Box" Recorder
**File:** `config/logging.conf`

Think of your script like an airplane. If it crashes while you are asleep, you need a "Black Box" to tell you what happened.

* **What it does:** It tells Python *how* to record events. It says: "Write errors to a file, but also print them to the screen. And please add the time and date to every line."
* **Why you need it:** Without this, your script runs silently. If it fails, you see nothing. With this, you get a file saying: `[2026-01-08 06:00] ERROR: Connection to Lender A timed out`.

#### Concept: Log Rotation
* **The Problem:** If you write to one file (`system.log`) forever, that file will eventually become 50GB. Opening it will crash your computer, and it will fill up your hard drive.
* **The Solution (Rotation):** It’s like keeping a diary.
    * When "Volume 1" gets full (e.g., reaches 5MB), you close it and put it on the shelf as `log.1`.
    * You start a fresh "Volume 2" (`system.log`).
    * If you decide to keep only 5 volumes, when you finish Volume 6, you throw Volume 1 in the trash.
    * **Result:** You always have recent history, but your hard drive never fills up.

---

### 2. The Configuration Files

**File:** `secrets.toml` (**The Safe**)
* **Analogy:** Your physical wallet.
* **Purpose:** Stores your passwords, database hostnames, and API keys.
* **Why:** You never want to paste passwords directly into your code (`.py` files). If you share your code with a colleague, you don't want to accidentally give them your passwords.

**File:** `config/gx_rules.yaml` (**The Rulebook**)
* **Analogy:** A checklist for a safety inspector.
* **Purpose:** A list of questions to ask the database.
    * *“Is application_id empty?”*
    * *“Is loan_amount less than 0?”*
* **Why:** If you want to add a new test, you just write a new line here. You don't have to touch the complicated Python code.

---

### 3. The "Source" Code (`src/`)

**File:** `src/gx_wrapper.py` (**The Inspector**)
* **Analogy:** The skilled worker who actually goes out and does the job.
* **Purpose:** It reads the **Rulebook** (`gx_rules.yaml`), picks the keys from the **Safe** (`secrets.toml`), drives to the **Database**, and checks if the data follows the rules. It doesn't decide *what* to do; it just follows instructions.

**File:** `src/notifier.py` (**The Messenger**)
* **Analogy:** The mail carrier.
* **Purpose:** Its only job is to take a list of failures, make them look pretty (put them in a table with red colors), and drop them in your email inbox.

**File:** `src/app.py` (**The Remote Control**)
* **Analogy:** A TV Remote or Dashboard.
* **Purpose:** This is for **YOU**. When you want to check the data *right now* (manually), you click buttons here. It calls the **Inspector** to do the work and shows the results on your screen instead of emailing them.

---

### 4. The Automation

**File:** `daily_job.py` (**The Alarm Clock**)
* **Analogy:** Your morning alarm.
* **Purpose:** This is the script Windows Task Scheduler kicks to wake everyone up. It says: *"Okay everyone, it's 6:00 AM! Inspector (`gx_wrapper`), go check all the lenders! Messenger (`notifier`), if he finds anything bad, email the boss!"*

**File:** `requirements.txt` (**The Shopping List**)
* **Analogy:** The ingredients list on a recipe.
* **Purpose:** It tells your computer: "To run this machine, you need to buy (install) `pandas`, `sqlalchemy`, and `great_expectations`."

---

### Summary of the Flow

1.  **Windows** wakes up `daily_job.py`.
2.  `daily_job.py` tells `gx_wrapper.py` to start working.
3.  `gx_wrapper.py` grabs the **passwords** (`secrets.toml`) and the **checklist** (`gx_rules.yaml`).
4.  It checks the databases and writes what it's doing to the **Diary** (`logs/`).
5.  If it finds broken data, it hands the list to `notifier.py`.
6.  `notifier.py` sends you an email.