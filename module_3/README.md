# Grad Cafe Data Analysis Project

## Project Overview

This project provides a full-stack solution for scraping graduate school admissions data from The Grad Cafe, cleaning and processing the data, correcting key fields with an LLM, storing it in a PostgreSQL database, and displaying a dynamic analysis on a Flask web page.

The application allows for an initial bulk scrape of historical data and features a web interface to pull new, incremental updates and refresh the analysis on demand.

---
## Setup and Installation

Follow these steps to get the project running locally.

### Prerequisites

* Python 3.10+
* PostgreSQL installed and running on your system.

### Installation Steps

1.  **Clone the Repository**
    ```bash
    git clone git@github.com:bparsha1/jhu_software_concepts.git
    cd jhu_software_concepts/module_3
    ```

2.  **Set Up a Virtual Environment** (Recommended)
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Create the PostgreSQL Database**
    Connect to PostgreSQL using `psql` and create the database.
    ```sql
    -- Connect as the postgres superuser
    sudo -u postgres psql

    -- Run the create command
    CREATE DATABASE grad_cafe;

    -- Quit psql
    \q
    ```
---
## How to Run

The project involves a one-time initial data load, followed by running the web application for ongoing analysis and updates.

### Part 1: Initial Data Load

This three-step process performs a scrape of the website up to 2020-01-01 and populates your database for the first time.

If you have llm-corrected data, you can skip step 1 and 2, and perform step 3.  You can replace llm_extend_applicant_data.json with whatever you're output file is named.

1.  **Scrape and Clean the Data**
    Run the `scrape_and_clean.py` script. This will perform a full scrape and save the clean, structured data into `new_structured_entries.json`.
    ```bash
    python scrape_and_clean.py
    ```

2.  **Process Data with the LLM**
    Run the LLM command. This reads the output from the previous step, runs your corrections, and saves the result to `llm_extend_applicant_data.json`.
    ```bash
    python llm_module/app.py --file new_structured_entries.json --stdout > llm_extend_applicant_data.json
    ```

3.  **Load Data into the Database**
    Run the `load_data.py` script, passing the LLM-corrected file as a command-line argument. This will create the table schema and perform the initial bulk insert.
    ```bash
    python load_data.py llm_extend_applicant_data.json
    ```
    Your database is now ready!

### Part 2: Run the Web Application

Start the Flask web server by running `app.py` without any arguments.
```bash
python app.py
```

Open your web browser and navigate to http://127.0.0.1:5000 to see the analysis page.

Using the buttons to pull will run `scrape_and_clean.py` followed by the llm, which should be stored in your directory under llm_module, and then `load_new_data.py` which puts the new data into the database.

# Testing the Data Pipeline

This test plan lets you run Pull New Data pipeline by deleting your 20 most recent records, you can verify that the "Pull New Data" button correctly finds, processes, and loads them back into your database.

---
## Step 1: Delete the 20 Most Recent Entries

First, you'll connect to your database using the `psql` terminal and run a SQL command to remove the newest data.

1.  **Connect to your database** by opening a terminal and running:
    ```bash
    psql -d grad_cafe -U postgres
    ```

2.  **Run the delete command.** This command finds the 20 entries with the most recent `date_added` and deletes them.
    ```sql
    DELETE FROM applicants WHERE pid IN (
      SELECT pid FROM applicants ORDER BY date_added DESC, pid DESC LIMIT 20
    );
    ```
    The terminal should respond with `DELETE 20`.

3.  **Exit psql.**
    ```sql
    \q
    ```

---
## Step 2: Run the Pipeline via the Web UI

Now, use the interface to trigger the pipeline and re-acquire the data you just deleted.

1.  **Click the "Pull New Data" button.**
    * You will see an immediate confirmation message on the webpage.
    * In the terminal where you launched `app.py`, you should see the pipeline's log messages start to appear (e.g., "--- STARTING HYBRID DATA PIPELINE ---", "Scraping page 1...").

2.  **Wait for the process to complete.** This may take several minutes. The "Update Analysis" button will be disabled during this time. Once the pipeline finishes, the button will become clickable again.

3.  **Click the "Update Analysis" button.** The page will refresh, showing the latest data from the database.

---
## Step 3: Verify the Results

Finally, confirm that the data was successfully re-added to your database.

**Check the application count on the webpage.** The "Applicant count" for the current year should now reflect the newly added data.