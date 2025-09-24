import os
import sys
import json
import psycopg

# --- database connection string ---
DB_CONN_STR = "dbname=grad_cafe user=postgres"

def setup_database(db_conn_str):
    """Create the applicants table in the database if it doesn't exist.
    
    This function initializes the database schema by creating the applicants
    table with all required columns for storing graduate school application
    data. The table includes fields for applicant information, academic metrics,
    application status, and LLM-generated university/program classifications.
    
    The function uses CREATE TABLE IF NOT EXISTS to ensure idempotent operation,
    allowing it to be called multiple times without error.
    
    :param db_conn_str: Database connection string for establishing the connection.
    :type db_conn_str: str
    """
    with psycopg.connect(db_conn_str) as conn: 
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS applicants (
                    pid INTEGER PRIMARY KEY,
                    program TEXT,
                    comments TEXT,
                    date_added DATE,
                    url TEXT,
                    status TEXT,
                    term TEXT,
                    us_or_international TEXT,
                    gpa FLOAT,
                    gre FLOAT,
                    gre_v FLOAT,
                    gre_aw FLOAT,
                    degree TEXT,
                    llm_generated_program TEXT,
                    llm_generated_university TEXT
                );
            """)
    print(f"Database table 'applicants' is ready on connection: {db_conn_str}")

def load_initial_json_data(file_path, db_conn_str):
    """Read cleaned JSON data from file and perform bulk load into database.
    
    This function handles the initial loading of processed applicant data from
    a JSONL file into the database. It reads the file line by line, parses
    each JSON entry, and performs bulk insertion with conflict resolution.
    
    The function includes comprehensive error handling for file operations and
    JSON parsing errors. It uses ON CONFLICT (pid) DO NOTHING to handle
    duplicate entries gracefully during the initial data load process.
    
    :param file_path: Path to the JSONL file containing applicant data to load.
    :type file_path: str
    :param db_conn_str: Database connection string for establishing the connection.
    :type db_conn_str: str
    """
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip(): data.append(json.loads(line))
    except FileNotFoundError:
        print(f"Error: '{file_path}' not found.")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return
    if not data:
        print("Data file is empty.")
        return
        
    # Connect to the database.
    with psycopg.connect(db_conn_str) as conn: 
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO applicants (
                    pid, program, comments, date_added, url, status, term,
                    us_or_international, gpa, gre, gre_v, gre_aw, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pid) DO NOTHING;
            """
            insert_count = 0
            for entry in data:
                llm_uni = entry.get('llm-generated-university', '')
                llm_prog = entry.get('llm-generated-program', '')
                record = (
                    entry.get('pid'), f"{llm_uni}, {llm_prog}", entry.get('comments'),
                    entry.get('date_added'), entry.get('url'), entry.get('status'),
                    entry.get('term'), entry.get('us_or_international'), entry.get('gpa'),
                    entry.get('gre'), entry.get('gre_v'), entry.get('gre_aw'),
                    entry.get('degree'), llm_prog, llm_uni
                )
                cur.execute(insert_query, record)
                insert_count += cur.rowcount
    print(f"Initial load complete. Added {insert_count} new record(s).")


# This function underlying is tested but __main__ can't be tested with pytest.
if __name__ == "__main__": # pragma: no cover
    if len(sys.argv) < 2:
        print("Usage: python load_data.py <path_to_json_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    
    # Update the calls to pass the default connection string.
    setup_database(DB_CONN_STR) 
    load_initial_json_data(input_file, DB_CONN_STR)