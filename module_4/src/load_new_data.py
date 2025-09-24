import json
import psycopg

# Input from the LLM output.
INPUT_FILE = 'new_structured_entries.json.jsonl'

def main(conn):
    """Load cleaned data from LLM output file into the database.
    
    This function serves as the main entry point for loading processed applicant
    data from the LLM-generated JSONL file into the database. It handles file
    reading, JSON parsing, error handling for malformed data, and database
    insertion with conflict resolution.
    
    The function reads the JSONL file line by line, parsing each entry and
    inserting it into the applicants table. It uses ON CONFLICT (pid) DO NOTHING
    to handle duplicate entries gracefully, ensuring idempotent operation.
    
    :param conn: Database connection object for inserting the loaded data.
    :type conn: psycopg.Connection
    """
    print("--- Starting Load Data Step ---")

    data = []
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # Skip any blank lines.
                    if not line.strip():
                        continue
                    # Parse each line as its own JSON object.
                    entry = json.loads(line)
                    data.append(entry)
                except json.JSONDecodeError:
                    print(f"Warning: Skipping malformed JSON line: {line.strip()}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Did the LLM script run correctly?")
        return

    if not data:
        print("No new data to load.")
        return

    # Uses the connection passed in from app.py.
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

            # Build the record tuple in the specified order.
            record = (
                entry.get('pid'),
                f"{llm_uni}, {llm_prog}", # The combined field for the 'program' column.
                entry.get('comments'),
                entry.get('date_added'),
                entry.get('url'),
                entry.get('status'),
                entry.get('term'),
                entry.get('us_or_international'),
                entry.get('gpa'),
                entry.get('gre'),
                entry.get('gre_v'),
                entry.get('gre_aw'),
                entry.get('degree'),
                llm_prog,  # Separate LLM-generated program.
                llm_uni    # Separate LLM-generated university.
            )
            cur.execute(insert_query, record)
            # Check if a row was actually inserted.
            insert_count += cur.rowcount

    print(f"Successfully inserted {insert_count} new entries out of {len(data)} total.")
    print("--- Finished Load Data Step ---")


# This function underlying is tested but __main__ can't be tested with pytest.
if __name__ == "__main__": # pragma: no cover
    DB_CONN_STR = "dbname=grad_cafe user=postgres"
    print("Running load_new_data.py as a standalone script...")
    with psycopg.connect(DB_CONN_STR) as connection:
        main(connection)
        connection.commit() # Save changes when run standalone.
    print("Standalone run complete.")