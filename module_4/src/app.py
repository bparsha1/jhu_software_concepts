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
    """Execute the complete data processing pipeline from scraping to database loading.
    
    This function orchestrates the entire data pipeline process, including scraping
    new entries, processing them through the LLM for data cleaning and standardization,
    and loading the results into the database. It manages the global pipeline state
    and provides comprehensive error handling for each pipeline stage.
    
    The pipeline consists of three main steps:
    1. Scraping new entries from the target website
    2. Processing scraped data through LLM for cleaning (only if new entries found)
    3. Loading processed data into the database (only if new entries found)
    
    The function uses the database connection string from Flask app configuration
    and handles subprocess execution for the LLM processing step.
    """
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
    """Redirect root URL to the analysis page.
    
    This route provides a convenient redirect from the root URL to the main
    analysis page, eliminating the need for users to manually navigate to
    the analysis interface.
    
    :returns: Flask redirect response to the analysis route.
    :rtype: werkzeug.wrappers.Response
    """
    return redirect(url_for('analysis'))

@app.route("/analysis")
def analysis():
    """Serve the main analysis page with current database query results.
    
    This route executes all predefined analysis queries against the database
    and renders the main analysis page template with the results. It provides
    the primary interface for viewing graduate school application analytics.
    
    The function executes all 10 analysis queries (q1-q10) and passes the
    results to the template for rendering. It includes error handling for
    database connection issues and query execution problems.
    
    :returns: Rendered HTML template with query results or error message with 500 status.
    :rtype: str or tuple[str, int]
    """
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
    """Re-execute database queries and return results as JSON for dynamic page updates.
    
    This route provides an API endpoint for refreshing analysis data without
    requiring a full page reload. It executes all analysis queries and returns
    the results as JSON data that can be consumed by client-side JavaScript
    for dynamic content updates.
    
    The function checks if a data pipeline is currently in progress and returns
    a conflict status if so. Otherwise, it executes all queries and returns
    the results in JSON format.
    
    :returns: JSON response with query results, error message, or conflict status.
    :rtype: flask.Response
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
    """Initiate the full data pipeline to pull and process new application data.
    
    This route triggers the complete data pipeline process, including scraping
    new entries, LLM processing, and database loading. It manages the global
    pipeline state to prevent concurrent pipeline executions and provides
    status feedback to the client.
    
    The function checks if a pipeline is already in progress and returns a
    conflict status if so. Otherwise, it sets the pipeline state and executes
    the full pipeline process.
    
    :returns: JSON response with success message or conflict/error status.
    :rtype: flask.Response
    """
    global pipeline_in_progress
    if pipeline_in_progress:
        return jsonify({"status": "error", "message": "A data pull is already in progress."}), 409
    
    pipeline_in_progress = True
    run_full_pipeline()
    return jsonify({"status": "success", "message": "Data pipeline completed."})

@app.route("/status")
def status():
    """Return the current status of the data pipeline process.
    
    This route provides a simple API endpoint for checking whether the data
    pipeline is currently running. It returns JSON data indicating the
    current pipeline state, which can be used by client-side code to
    provide appropriate user feedback and interface updates.
    
    :returns: JSON response with pipeline status boolean.
    :rtype: flask.Response
    """
    return jsonify({"pipeline_in_progress": pipeline_in_progress})


if __name__ == "__main__": # pragma: no cover
    app.run(debug=True)