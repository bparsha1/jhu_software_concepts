from flask import Flask, render_template, jsonify, redirect, url_for
import psycopg
import subprocess
from . import query_data
from .scrape_and_clean import main as run_scrape_and_clean
from .load_new_data import main as run_data_loading

app = Flask(__name__)

# Use Flask's config system. This can be set during testing.
app.config.from_mapping(
    DATABASE_URI="dbname=grad_cafe user=postgres"
)

pipeline_in_progress = False

def run_full_pipeline():
    """Runs the full data pipeline."""
    global pipeline_in_progress
    print("--- STARTING PIPELINE ---")
    
    try:
        # Use the connection string from the app's config
        conn_str = app.config['DATABASE_URI']
        with psycopg.connect(conn_str) as conn:
            print("Pipeline: Database connection established.")
            
            print("Step 1/3: Scraping new entries...")
            new_entries_count = run_scrape_and_clean(conn) 
            print(f"Scraping complete. Found {new_entries_count} new entries.")

            if new_entries_count > 0:
                print("Step 2/3: Cleaning scraped data via subprocess...")
                command_args = ["python", "src/llm_hosting/app.py", "--file", "new_structured_entries.json"]
                subprocess.run(command_args, capture_output=True, text=True, check=True)
                print("Cleaning complete.")

                print("Step 3/3: Loading data into database...")
                run_data_loading(conn) 
                conn.commit()
                print("Data loading complete.")
            else:
                print("Skipping LLM and data loading steps as no new entries were found.")
        print("--- DATA PIPELINE FINISHED SUCCESSFULLY ---")

    except subprocess.CalledProcessError as e:
        print(f"--- PIPELINE FAILED: Subprocess error: {e.stderr} ---")
    except Exception as e:
        print(f"An unexpected error occurred in the pipeline: {e}")
    finally:
        print("--- PIPELINE PROCESS HAS CONCLUDED ---")
        pipeline_in_progress = False

# --- Flask Routes ---


# Root level redirects to analysis so we don't need
# to have the user choose analysis.
@app.route("/")
def index():
    """ Redirects to the analysis page. """
    return redirect(url_for('analysis'))

@app.route("/analysis")
def analysis():
    """Serves the main analysis page with data from the database."""
    try:
        conn_str = app.config['DATABASE_URI']
        with psycopg.connect(conn_str) as conn:
            results = {
                "q1": query_data.execute_query(conn, query_data.q1),
                "q2": query_data.execute_query(conn, query_data.q2),
                "q3": query_data.execute_query(conn, query_data.q3),
                "q4": query_data.execute_query(conn, query_data.q4),
                "q5": query_data.execute_query(conn, query_data.q5),
                "q6": query_data.execute_query(conn, query_data.q6),
                "q7": query_data.execute_query(conn, query_data.q7),
                "q8": query_data.execute_query(conn, query_data.q8),
                "q9": query_data.execute_query(conn, query_data.q9, fetch="all"),
                "q10": query_data.execute_query(conn, query_data.q10, fetch="all")
            }
        return render_template("index.html", results=results)
    except Exception as e:
        print(f"Error during page load query: {e}")
        return "Error loading data from the database.", 500

@app.route("/update-analysis", methods=['GET', 'POST'])
def update_analysis():
    """
    Re-runs the database queries and returns the results as JSON.
    This is called by JavaScript to refresh the page content.
    """
    if pipeline_in_progress:
        return jsonify({
            "error": "A data pull is in progress. Please wait."
        }), 409 # Return 409 Conflict
    try:
        conn_str = app.config['DATABASE_URI']
        with psycopg.connect(conn_str) as conn:
            results = {
                "q1": query_data.execute_query(conn, query_data.q1),
                "q2": query_data.execute_query(conn, query_data.q2),
                "q3": query_data.execute_query(conn, query_data.q3),
                "q4": query_data.execute_query(conn, query_data.q4),
                "q5": query_data.execute_query(conn, query_data.q5),
                "q6": query_data.execute_query(conn, query_data.q6),
                "q7": query_data.execute_query(conn, query_data.q7),
                "q8": query_data.execute_query(conn, query_data.q8),
                "q9": query_data.execute_query(conn, query_data.q9, fetch="all"),
                "q10": query_data.execute_query(conn, query_data.q10, fetch="all")
            }
        # Instead of rendering a template, we return the data as JSON
        return jsonify(results)
    except Exception as e:
        print(f"Error during analysis update query: {e}")
        return jsonify({"error": "Error loading data from the database."}), 500


@app.route("/pull-data", methods=["POST"])
def pull_data():
    """ 
    This runs the pipeline to pull down any new data. 
    """
    global pipeline_in_progress
    if pipeline_in_progress:
        return jsonify({"status": "error", "message": "A data pull is already in progress."}), 409
    
    pipeline_in_progress = True
    run_full_pipeline()
    return jsonify({"status": "success", "message": "Data pipeline completed."})

@app.route("/status")
def status():
    """
    This lets you know if the pipeline is currently running.
    """
    return jsonify({"pipeline_in_progress": pipeline_in_progress})


if __name__ == "__main__": # pragma: no cover
    app.run(debug=True)