import os
import sys
import json
import psycopg

# --- database connection string ---
DB_CONN_STR = "dbname=grad_cafe user=postgres"

def setup_database(db_conn_str): # <--- CHANGED
    """
    Creates the applicants table in the database if it doesn't exist.
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

# This function also accepts the connection string.
def load_initial_json_data(file_path, db_conn_str): 
    """
    Reads cleaned JSON data and performs a bulk load into the database.
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