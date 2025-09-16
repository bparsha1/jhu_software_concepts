import os
import sys
import json
import psycopg

# --- database connection string ---
DB_CONN_STR = "dbname=grad_cafe user=postgres"

def setup_database():
    """Creates the applicants table in the database if it doesn't exist."""
    with psycopg.connect(DB_CONN_STR) as conn:
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
    print("Database table 'applicants' is ready.")

def load_initial_json_data(file_path):
    """Reads cleaned JSON data and performs a bulk load into the database."""
    print(f"Loading initial data from {file_path}...")
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Skip empty lines
                if line.strip():
                    data.append(json.loads(line))
    except FileNotFoundError:
        print(f"Error: '{file_path}' not found. Please check the path and try again.")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON. There may be a malformed line in your file: {e}")
        return

    if not data:
        print("Data file is empty or could not be parsed. Nothing to load.")
        return
        
    print(f"Found {len(data)} records in the JSON file.")

    with psycopg.connect(DB_CONN_STR) as conn:
        with conn.cursor() as cur:

            insert_query = """
                INSERT INTO applicants (
                    pid, 
                    program, 
                    comments, 
                    date_added, 
                    url, 
                    status, 
                    term,
                    us_or_international, 
                    gpa, 
                    gre, 
                    gre_v, 
                    gre_aw, 
                    degree,
                    llm_generated_program, 
                    llm_generated_university
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pid) DO NOTHING;
            """

            insert_count = 0
            for entry in data:
                llm_uni = entry.get('llm-generated-university', '')
                llm_prog = entry.get('llm-generated-program', '')

                record = (
                    entry.get('pid'),
                    f"{llm_uni}, {llm_prog}", # The combined field for 'program'
                    entry.get('comments'),
                    entry.get('date_added'),
                    entry.get('url'),
                    entry.get('status'),
                    entry.get('semester_and_year'),
                    entry.get('student_type'),
                    entry.get('gpa'),
                    entry.get('gre'),
                    entry.get('gre_v'),
                    entry.get('gre_aw'),
                    entry.get('degree'),
                    llm_prog,  # Separate LLM-generated program
                    llm_uni    # Separate LLM-generated university
                )
                cur.execute(insert_query, record)
                insert_count += cur.rowcount

    print(f"Initial load complete. Added {insert_count} new record(s) to the database.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_data.py <path_to_json_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    
    setup_database()
    load_initial_json_data(input_file)