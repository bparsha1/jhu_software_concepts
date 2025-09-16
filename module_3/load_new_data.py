import json
import psycopg

# Input from the LLM output
INPUT_FILE = 'new_structured_entries.json.jsonl' # Corrected based on your app.py logic

def main(conn):
    """
    Loads cleaned data from the LLM's output file into the database
    using the provided connection. This is the function app.py imports.
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
                    # Parse each line as its own JSON object
                    entry = json.loads(line)
                    data.append(entry)
                except json.JSONDecodeError:
                    print(f"Warning: Skipping malformed JSON line: {line.strip()}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found. Did the LLM script run correctly?")
        return
    # --- END MODIFIED SECTION ---

    if not data:
        print("No new data to load.")
        return

    # Uses the connection passed in from app.py
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

            # Build the record tuple in the specified order
            record = (
                entry.get('pid'),
                f"{llm_uni}, {llm_prog}", # The combined field for the 'program' column
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
            # Check if a row was actually inserted
            insert_count += cur.rowcount

    print(f"Successfully inserted {insert_count} new entries out of {len(data)} total.")
    print("--- Finished Load Data Step ---")


# Main for testing.
if __name__ == '__main__':
    DB_CONN_STR = "dbname=grad_cafe user=postgres"
    print("Running load_new_data.py as a standalone script...")
    with psycopg.connect(DB_CONN_STR) as connection:
        main(connection)
        connection.commit() # Save changes when run standalone
    print("Standalone run complete.")