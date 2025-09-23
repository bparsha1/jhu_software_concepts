import psycopg
import pytest
import json
from src import app, load_new_data, scrape_and_clean
from src.load_data import load_initial_json_data
from src.load_new_data import main as load_new_data_main
from src.query_data import execute_query, run_all_queries_for_console


# This is a sample of fake data that mimics the output of the scraper/LLM.
FAKE_ENTRY_DATA = [
    {
        'pid': 103, 'comments': 'Great experience.', 'date_added': '2025-09-01',
        'url': 'http://example.com/101', 'status': 'Accepted', 'term': 'Fall 2025',
        'us_or_international': 'American', 'gpa': 3.2, 'gre': 319, 'gre_v': 159,
        'gre_aw': 4.1, 'degree': 'PhD', 'llm_generated_program': 'Mechanical Engineering',
        'llm_generated_university': 'Post University'
    },
    {
        'pid': 104, 'comments': 'Number one fun.', 'date_added': '2025-09-01',
        'url': 'http://example.com/101', 'status': 'Accepted', 'term': 'Fall 2025',
        'us_or_international': 'American', 'gpa': 4.0, 'gre': 310, 'gre_v': 150,
        'gre_aw': 4.2, 'degree': 'MS', 'llm_generated_program': 'Computer Science',
        'llm_generated_university': 'Get University'
    }
]


def create_fake_jsonl_file(tmp_path, data):
    """Helper function to create a JSONL file in a temporary directory."""
    jsonl_file = tmp_path / "new_structured_entries.json.jsonl"
    with open(jsonl_file, 'w') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return str(jsonl_file)


@pytest.mark.db
def test_insert_on_pull(client, db_session, mocker, tmp_path):
    """
    After POST/pull-data new rows exist.  Start with a clean database
    and do a fresh pull with /pull-data.  After the pull the database
    should be populated.
    """
    # Ensure DB is empty and create the fake data file.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0, "Database should be empty before the test."

    fake_file_path = create_fake_jsonl_file(tmp_path, FAKE_ENTRY_DATA)
    mocker.patch('src.load_new_data.INPUT_FILE', fake_file_path) # Point loader to our fake file.
    mocker.patch('src.app.run_scrape_and_clean', return_value=len(FAKE_ENTRY_DATA))
    mocker.patch('subprocess.run') # Mock the LLM subprocess call.

    # Call the endpoint that triggers the database write.
    response = client.post('/pull-data')
    assert response.status_code == 200

    # Check that the data now exists in the database.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2
        
        # Check for a pid that exists (103) and the correct comment.
        cur.execute("SELECT comments FROM applicants WHERE pid = 103;")
        assert cur.fetchone()[0] == "Great experience."


@pytest.mark.db
def test_idempotency_constraints(client, db_session, mocker, tmp_path):
    """
    Duplicate pulls do not duplicate rows.
    """
    # Run the pull once to populate the database.
    fake_file_path = create_fake_jsonl_file(tmp_path, FAKE_ENTRY_DATA)
    mocker.patch('src.load_new_data.INPUT_FILE', fake_file_path)
    mocker.patch('src.app.run_scrape_and_clean', return_value=len(FAKE_ENTRY_DATA))
    mocker.patch('subprocess.run')
    
    client.post('/pull-data')
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2, "Database should have 2 rows after the first pull."

    # Run the pull a second time with the exact same data.
    client.post('/pull-data')

    # The number of rows should NOT have changed.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2, "Row count should not change on second pull with same data."


@pytest.mark.db
def test_simple_query_function(db_with_data):
    """
    Test simple query function returns expected data.
    """
    # The db_with_data fixture provides a connection to a populated DB.
    conn = db_with_data
    
    # Check that a query that fetches one row.
    result_one = execute_query(conn, "SELECT COUNT(*) FROM applicants;")
    assert isinstance(result_one, tuple)
    assert result_one[0] == 2

    # Check a query that fetches all rows.
    result_all = execute_query(
        conn, "SELECT pid, status FROM applicants ORDER BY pid;", fetch="all"
    )
    assert isinstance(result_all, list)
    assert len(result_all) == 2
    assert result_all[0] == (101, 'Accepted')
    assert result_all[1] == (102, 'Rejected')


@pytest.mark.db
def test_pull_with_no_new_entries(client, db_session, mocker):
    """
    Test pull with no new entries: Pipeline skips steps if scraper finds nothing.
    """
    # Start with an empty DB and mock the pipeline functions.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0, "Database should be empty before the test."

    # Simulate the scraper finding 0 new entries.
    mocker.patch('src.app.run_scrape_and_clean', return_value=0)
    
    # Mock the other pipeline steps to verify they are NOT called.
    mock_subprocess = mocker.patch('subprocess.run')
    mock_data_loading = mocker.patch('src.app.run_data_loading')

    # Call the endpoint.
    response = client.post('/pull-data')
    assert response.status_code == 200

    # The database should still be empty.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0, "Row count should still be 0."

    # Assert that the skipped steps were, in fact, not called.
    mock_subprocess.assert_not_called()
    mock_data_loading.assert_not_called()


@pytest.mark.db
def test_load_initial_data_success(db_session, tmp_path):
    """
    Tests the successful "happy path": a valid file is provided, a connection
    is made, and data is inserted correctly.
    """
    # Create a temporary file with valid data.
    test_file = tmp_path / "good_data.jsonl"
    with open(test_file, 'w') as f:
        for item in FAKE_ENTRY_DATA:
            f.write(json.dumps(item) + '\n')
    
    # Call the function with the path to our fake file and a real test DB connection.
    load_initial_json_data(str(test_file), db_session.info.dsn)

    # Check that the data was inserted into the database.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2
        
        # Check for data from FAKE_ENTRY_DATA.
        cur.execute("SELECT gpa FROM applicants WHERE pid = 104;")
        assert cur.fetchone()[0] == 4.0


@pytest.mark.db
def test_load_initial_data_db_error(mocker, tmp_path):
    """
    Tests that an exception during `psycopg.connect` is handled correctly.
    """
    # Create a valid data file, so the function tries to connect.
    test_file = tmp_path / "good_data.jsonl"
    with open(test_file, 'w') as f:
        f.write(json.dumps(FAKE_ENTRY_DATA[0]) + '\n')
    
    # Mock the database connection to fail.
    mocker.patch(
        'psycopg.connect', 
        side_effect=psycopg.OperationalError("Simulated DB connection failed")
    )

    # Confirm that the expected exception is raised.
    with pytest.raises(psycopg.OperationalError, match="Simulated DB connection failed"):
        load_initial_json_data(str(test_file), "fake_connection_string")  


@pytest.mark.db
def test_load_initial_data_with_empty_file(db_session, tmp_path, capsys):
    """
    Tests the `if not data:` branch by providing an empty file.
    It should print a message and not insert any data.
    """
    # Create a temporary file that is completely empty.
    empty_file = tmp_path / "empty.jsonl"
    empty_file.touch() # Creates a 0-byte file

    # Call the function with the empty file.
    load_initial_json_data(str(empty_file), db_session.info.dsn)

    # Check that the database is still empty.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0

    # Check that the correct message was printed to the console.
    captured = capsys.readouterr()
    assert "Data file is empty." in captured.out


@pytest.mark.db
def test_load_new_data_skips_blank_lines(db_session, tmp_path, mocker):
    """
    Tests that the data loader correctly skips blank or whitespace-only lines
    in the input file, using the existing FAKE_ENTRY_DATA.
    """
    # Create a file with FAKE_ENTRY_DATA and blank lines.
    # We write the file manually here to insert the blank lines for the test.
    test_file = tmp_path / "data_with_blanks.jsonl"
    with open(test_file, 'w') as f:
        f.write(json.dumps(FAKE_ENTRY_DATA[0]) + '\n') # First valid record
        f.write('\n')                                  # A blank line
        f.write('   \n')                               # A whitespace-only line
        f.write(json.dumps(FAKE_ENTRY_DATA[1]) + '\n') # Second valid record

    # Use mocker to point the script to our temporary file
    mocker.patch('src.load_new_data.INPUT_FILE', str(test_file))

    # Call the function that reads the file
    load_new_data_main(db_session)

    # Check that only the two valid records were inserted
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2

        # Verify a value from the second record to be sure
        cur.execute("SELECT comments FROM applicants WHERE pid = 104;")
        assert cur.fetchone()[0] == "Number one fun."


@pytest.mark.db
def test_load_new_data_handles_malformed_json(db_session, tmp_path, mocker, capsys):
    """
    Tests that the data loader correctly skips a line with malformed JSON
    and prints a warning, covering the JSONDecodeError except block.
    """
    # Create a file with valid and invalid JSON.
    valid_record_1 = json.dumps({"pid": 101, "gpa": 3.9})
    malformed_line = '{"pid": 201, "gpa": 3.7,}' # Invalid due to trailing comma
    valid_record_2 = json.dumps({"pid": 102, "gpa": 3.5})

    file_content = f"{valid_record_1}\n{malformed_line}\n{valid_record_2}\n"

    test_file = tmp_path / "malformed_data.jsonl"
    test_file.write_text(file_content)

    # Use mocker to point the script to our test file.
    mocker.patch('src.load_new_data.INPUT_FILE', str(test_file))

    # Call the function that reads the file.
    load_new_data_main(db_session)

    # Check that only the two valid records were inserted.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2

    # Check that the specific warning message was printed to the console.
    captured = capsys.readouterr()
    assert "Warning: Skipping malformed JSON line" in captured.out
    assert malformed_line in captured.out


@pytest.mark.db
def test_load_new_data_empty_file(db_session, mocker):
    """
    Test `load_new_data` handles an empty input file.
    """
    mocker.patch("builtins.open", mocker.mock_open(read_data=""))
    load_new_data.main(db_session)
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0


@pytest.mark.db
def test_load_initial_data_errors(mocker):
    """
    Test `load_data` script handles file and JSON errors.
    """
    # Test FileNotFoundError
    load_initial_json_data("non_existent_file.json", "")

    # Test JSONDecodeError
    mocker.patch("builtins.open", mocker.mock_open(read_data="{bad json}"))
    mocker.patch("psycopg.connect") # Mock connection to avoid DB error
    load_initial_json_data("fake_file.json", "")


@pytest.mark.db
def test_get_latest_day_info_db_error(mocker):
    """
    Test `get_latest_day_info` handles a psycopg error.
    """
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.side_effect = psycopg.Error("DB connection failed")
    
    latest_date, pids = scrape_and_clean.get_latest_day_info(mock_conn)
    assert latest_date is None
    assert pids == set()


@pytest.mark.db
def test_load_new_data_file_not_found(db_session, mocker):
    """
    Test `load_new_data` handles FileNotFoundError gracefully.
    """
    mocker.patch("builtins.open", side_effect=FileNotFoundError)
    # The function should run, print an error, but not crash.
    load_new_data.main(db_session)
    # No assertion needed, success is the test not raising an unhandled exception.


@pytest.mark.db
def test_run_all_queries_for_console(db_with_data, capsys):
    """
    run_all_queries_for_console should return formatted analysis to 
    the console.
    """
    # The db_with_data fixture provides the populated connection.
    conn = db_with_data

    # Call the function that prints to the console.
    run_all_queries_for_console(conn)

    # Capture the printed output from the console.
    captured = capsys.readouterr()
    output = captured.out

    # Check for a few key, representative strings in the output.
    # This confirms the queries ran and the formatting is correct.
    assert "--- Running Grad Cafe Data Analysis Queries ---" in output
    assert "Applicants for Fall 2025: 2" in output # Based on fixture data for Q1
    assert "Test University: 1 applications" in output # Based on fixture data for Q9
    assert "Accepted: 3.80" in output # Based on fixture data for Q10