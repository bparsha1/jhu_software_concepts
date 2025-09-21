from flask import Flask, render_template, jsonify
import psycopg
import subprocess
import query_data
from scrape_and_clean import main as run_scrape_and_clean
from load_new_data import main as run_data_loading

app = Flask(__name__)
DB_CONN_STR = "dbname=grad_cafe user=postgres"
pipeline_in_progress = False

def run_full_pipeline():
    """
    Runs the pipeline to scrape, clean, and load data, 
    and skips if no new data is found.
    """
    global pipeline_in_progress
    print("--- STARTING PIPELINE ---")
    
    try:
        with psycopg.connect(DB_CONN_STR) as conn:
            print("Pipeline: Database connection established.")
            
            # Scrape new raw data and capture the return value.
            print("Step 1/3: Scraping new entries...")
            new_entries_count = run_scrape_and_clean(conn) 
            print(f"Scraping complete. Found {new_entries_count} new entries.")

            # Only run the next steps if new entries were actually found
            if new_entries_count > 0:
                # Clean the raw data with llm.
                print("Step 2/3: Cleaning scraped data via subprocess...")
                
                # It was easier here to just accept the standard out
                # from the llm, which is ifile.jsonl than to redirect
                # stdout.
                command_args = [
                    "python", 
                    "llm_hosting/app.py", 
                    "--file", 
                    "new_structured_entries.json"
                ]

                clean_process = subprocess.run(
                    command_args, 
                    capture_output=True, text=True, check=True
                )
                print("Cleaning complete.")

                # Load the cleaned data.
                print("Step 3/3: Loading data into database...")
                run_data_loading(conn) 
                conn.commit()
                print("Data loading complete.")
            else:
                print("Skipping LLM and data loading steps as no new entries were found.")

        print("--- DATA PIPELINE FINISHED SUCCESSFULLY ---")

    # Setup warnings for debugging errors.
    except subprocess.CalledProcessError as e:
        print("--- PIPELINE FAILED: Error during the LLM subprocess step. ---")
        print(f"Return Code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
    except Exception as e:
        print(f"An unexpected error occurred in the pipeline: {e}")
    finally:
        print("--- PIPELINE PROCESS HAS CONCLUDED ---")
        # Flip the flag to false.
        pipeline_in_progress = False

# --- Flask Routes ---
@app.route("/")
def index():
    try:
        results = {
            "q1": query_data.execute_query(query_data.q1), 
            "q2": query_data.execute_query(query_data.q2),
            "q3": query_data.execute_query(query_data.q3), 
            "q4": query_data.execute_query(query_data.q4),
            "q5": query_data.execute_query(query_data.q5), 
            "q6": query_data.execute_query(query_data.q6),
            "q7": query_data.execute_query(query_data.q7), 
            "q8": query_data.execute_query(query_data.q8),
            "q9": query_data.execute_query(query_data.q9, fetch="all"),
            "q10": query_data.execute_query(query_data.q10, fetch="all")
        }
        return render_template("index.html", results=results)
    except Exception as e:
        print(f"Error during page load query: {e}")
        return "Error loading data from the database. Please try again in a moment.", 500

@app.route("/pull-data", methods=["POST"])
def pull_data():
    global pipeline_in_progress
    if pipeline_in_progress:
        return jsonify({"status": "error", "message": "A data pull is already in progress."}), 409
    
    pipeline_in_progress = True
    run_full_pipeline()
    return jsonify({"status": "success", "message": "Data pipeline completed."})

@app.route("/status")
def status():
    return jsonify({"pipeline_in_progress": pipeline_in_progress})

if __name__ == "__main__":
    app.run(debug=True)