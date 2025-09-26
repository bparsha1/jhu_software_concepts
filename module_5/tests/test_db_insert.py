import os
import psycopg
import pytest
import json
from datetime import date
from unittest.mock import MagicMock
from src import scrape_and_clean
from src.load_data import load_initial_json_data
from src.load_new_data import main as load_new_data_main
from src.query_data import execute_query, run_all_queries_for_console


# This is a sample of fake data that mimics the output of the scraper/LLM.
FAKE_ENTRY_DATA = [
    {
        'pid': 103, 
        'comments': 'Great experience.', 
        'date_added': '2025-09-01',
        'url': 'http://example.com/101', 
        'status': 'Accepted', 
        'term': 'Fall 2025',
        'us_or_international': 'American', 
        'gpa': 3.2, 
        'gre': 319, 
        'gre_v': 159,
        'gre_aw': 4.1, 
        'degree': 'PhD', 
        'llm-generated-program': 'Mechanical Engineering',
        'llm-generated-university': 'Post University'
    },
    {
        'pid': 104, 
        'comments': 'Number one fun.', 
        'date_added': '2025-09-01',
        'url': 'http://example.com/101', 
        'status': 'Accepted', 
        'term': 'Fall 2025',
        'us_or_international': 'American', 
        'gpa': 4.0, 
        'gre': 310, 
        'gre_v': 150,
        'gre_aw': 4.2, 
        'degree': 'MS', 
        'llm-generated-program': 'Computer Science',
        'llm-generated-university': 'Get University'
    }
]


def create_fake_jsonl_file(tmp_path, data):
    """Helper function to create a JSONL file in a temporary directory.
    
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    :param data: List of dictionaries to write to the JSONL file.
    :type data: list
    :returns: String path to the created JSONL file.
    :rtype: str
    """
    jsonl_file = tmp_path / "new_structured_entries.json.jsonl"
    with open(jsonl_file, 'w') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return str(jsonl_file)


def get_connection_string():
    """Get the appropriate connection string based on the environment.
    
    :returns: Connection string for test database.
    :rtype: str
    """
    if os.getenv('GITHUB_ACTIONS'):
        return "dbname=test_grad_cafe user=postgres password=postgres host=localhost port=5432"
    else:
        return "dbname=test_grad_cafe user=postgres"


@pytest.mark.db
def test_insert_on_pull(client, db_session, mocker, tmp_path):
    """Test that POST /pull-data inserts new rows into the database.
    
    This test starts with a clean database and performs a fresh pull with
    the /pull-data endpoint. After the pull, the database should be populated
    with the expected number of rows and correct data.
    
    :param client: Flask test client fixture for making HTTP requests.
    :type client: flask.testing.FlaskClient
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    """
    # Ensure DB is empty and create the fake data file.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0, "Database should be empty before the test."

    fake_file_path = create_fake_jsonl_file(tmp_path, FAKE_ENTRY_DATA)
    mocker.patch('src.load_new_data.INPUT_FILE', fake_file_path) # Point loader to our fake file.
    # Mock the scrape function to return the count
    mocker.patch('src.scrape_and_clean.main', return_value=len(FAKE_ENTRY_DATA))
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
    """Test that duplicate pulls do not duplicate rows in the database.
    
    This test verifies that running the pull operation multiple times with
    the same data does not create duplicate entries, ensuring the database
    constraints properly prevent data duplication.
    
    :param client: Flask test client fixture for making HTTP requests.
    :type client: flask.testing.FlaskClient
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    """
    # Run the pull once to populate the database.
    fake_file_path = create_fake_jsonl_file(tmp_path, FAKE_ENTRY_DATA)
    mocker.patch('src.load_new_data.INPUT_FILE', fake_file_path)
    mocker.patch('src.scrape_and_clean.main', return_value=len(FAKE_ENTRY_DATA))
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
    """Test that the simple query function returns expected data formats.
    
    This test verifies that the ``execute_query`` function correctly handles
    both single row and multiple row queries, returning the appropriate data
    types for each query type.
    
    :param db_with_data: Database fixture providing a populated database connection.
    :type db_with_data: psycopg.Connection
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
def test_pull_with_no_new_entries(client, db_session, mocker, test_db):
    """Test pull operation when scraper finds no new entries.
    
    This test verifies that the pipeline correctly skips subsequent steps
    when the scraper returns zero new entries, ensuring that downstream
    processing functions are not called unnecessarily.
    
    :param client: Flask test client fixture for making HTTP requests.
    :type client: flask.testing.FlaskClient
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param test_db: Test database connection string.
    :type test_db: str
    """
    # The db_session fixture should have cleaned the database
    # Let's verify it's actually empty
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        initial_count = cur.fetchone()[0]
        assert initial_count == 0, f"Database should be empty but has {initial_count} rows. Check db_session fixture."
    
    # Also check using a fresh connection to ensure consistency
    import psycopg
    with psycopg.connect(test_db) as fresh_conn:
        with fresh_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            fresh_count = cur.fetchone()[0]
            assert fresh_count == 0, f"Fresh connection shows {fresh_count} rows, db_session shows {initial_count}"
    
    # Mock the other pipeline steps BEFORE mocking the scraper
    # Mock subprocess.run where it's imported in app.py
    mock_subprocess = mocker.patch('src.app.subprocess.run')
    mock_data_loading = mocker.patch('src.app.run_data_loading')
    
    # Now simulate the scraper finding 0 new entries
    # This needs to be mocked where it's called in app.py
    mocker.patch('src.app.run_scrape_and_clean', return_value=0)

    # Call the endpoint.
    response = client.post('/pull-data')
    assert response.status_code == 200

    # The database should still be empty
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        final_count = cur.fetchone()[0]
        assert final_count == 0, f"Database should still be empty but has {final_count} rows"

    # Assert that the skipped steps were, in fact, not called.
    mock_subprocess.assert_not_called()
    mock_data_loading.assert_not_called()


@pytest.mark.db
def test_load_initial_data_success(db_session, tmp_path):
    """Test successful loading of initial data from a valid JSONL file.
    
    This test verifies the "happy path" scenario where a valid file is provided,
    a database connection is established, and data is correctly inserted into
    the database.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    """
    # Create a temporary file with valid data.
    test_file = tmp_path / "good_data.jsonl"
    with open(test_file, 'w') as f:
        for item in FAKE_ENTRY_DATA:
            f.write(json.dumps(item) + '\n')
    
    # Use environment-appropriate connection string
    conn_str = get_connection_string()
    load_initial_json_data(str(test_file), conn_str)

    # Check that the data was inserted into the database.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2
        
        # Check for data from FAKE_ENTRY_DATA.
        cur.execute("SELECT gpa FROM applicants WHERE pid = 104;")
        assert cur.fetchone()[0] == 4.0


@pytest.mark.db
def test_load_initial_data_db_error(mocker, tmp_path):
    """Test that database connection errors are handled correctly during initial data loading.
    
    This test verifies that when ``psycopg.connect`` raises an exception,
    the function properly propagates the error without causing unhandled
    exceptions.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
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
    """Test loading initial data from an empty file.
    
    This test verifies the ``if not data:`` code branch by providing a
    completely empty file. The function should print an appropriate message
    and not attempt to insert any data into the database.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """
    # Create a temporary file that is completely empty.
    empty_file = tmp_path / "empty.jsonl"
    empty_file.touch() # Creates a 0-byte file

    # Use environment-appropriate connection string
    conn_str = get_connection_string()
    load_initial_json_data(str(empty_file), conn_str)

    # Check that the database is still empty.
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0

    # Check that the correct message was printed to the console.
    captured = capsys.readouterr()
    assert "Data file is empty." in captured.out


@pytest.mark.db
def test_load_new_data_skips_blank_lines(db_session, tmp_path, mocker):
    """Test that the data loader correctly skips blank or whitespace-only lines.
    
    This test verifies that when processing a JSONL file containing blank lines
    and whitespace-only lines, the loader correctly skips these lines and only
    processes valid JSON entries.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
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
    """Test that the data loader correctly handles malformed JSON lines.
    
    This test verifies that when the data loader encounters a line with
    malformed JSON, it correctly skips the line, prints a warning message,
    and continues processing valid entries. This covers the JSONDecodeError
    exception handling block.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
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
    """Test that ``load_new_data`` handles an empty input file gracefully.
    
    This test verifies that when the input file is empty, the function
    processes it without errors and leaves the database unchanged.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    mocker.patch("builtins.open", mocker.mock_open(read_data=""))
    load_new_data_main(db_session)
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 0


@pytest.mark.db
def test_load_initial_data_errors(mocker):
    """Test that ``load_data`` script handles file and JSON errors gracefully.
    
    This test verifies that the function can handle both FileNotFoundError
    and JSONDecodeError exceptions without crashing, ensuring robust error
    handling for various file-related issues.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Test FileNotFoundError
    load_initial_json_data("non_existent_file.json", "")

    # Test JSONDecodeError
    mocker.patch("builtins.open", mocker.mock_open(read_data="{bad json}"))
    mocker.patch("psycopg.connect") # Mock connection to avoid DB error
    load_initial_json_data("fake_file.json", "")


@pytest.mark.db
def test_get_latest_day_info_db_error(mocker):
    """Test that ``get_latest_day_info`` handles database errors gracefully.
    
    This test verifies that when a psycopg error occurs during database
    operations, the function returns appropriate default values instead
    of crashing.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.side_effect = psycopg.Error("DB connection failed")
    
    latest_date, pids = scrape_and_clean.get_latest_day_info(mock_conn)
    assert latest_date is None
    assert pids == set()


@pytest.mark.db
def test_load_new_data_file_not_found(db_session, mocker):
    """Test that ``load_new_data_main`` handles FileNotFoundError gracefully.

    This test ensures that if the target input file for ``load_new_data_main``
    does not exist, the function catches the ``FileNotFoundError`` and exits
    gracefully without raising an unhandled exception.

    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    mocker.patch("builtins.open", side_effect=FileNotFoundError)
    # The function should run, print an error, but not crash.
    load_new_data_main(db_session)
    # No assertion needed, success is the test not raising an unhandled exception.


@pytest.mark.db
def test_run_all_queries_for_console(db_with_data, capsys):
    """Test that ``run_all_queries_for_console`` returns formatted analysis output.
    
    This test verifies that the function executes all analysis queries and
    formats the output correctly for console display, ensuring that key
    analysis results are properly presented to the user.
    
    :param db_with_data: Database fixture providing a populated database connection.
    :type db_with_data: psycopg.Connection
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
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


@pytest.mark.db
def test_scrape_and_clean_main_function(db_session, mocker):
    """Test the main function in scrape_and_clean module.
    
    This test verifies that the main function correctly coordinates
    the scraping process, including robot.txt checking, database querying,
    and file output.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock the robot parser to allow scraping
    mock_robot_parser = mocker.patch('src.scrape_and_clean.RobotFileParser')
    mock_rp_instance = MagicMock()
    mock_rp_instance.can_fetch.return_value = True
    mock_robot_parser.return_value = mock_rp_instance
    
    # Mock the scrape_and_clean function to return test data
    mock_scrape = mocker.patch('src.scrape_and_clean.scrape_and_clean')
    mock_scrape.return_value = [
        {'pid': 201, 'university': 'Test U', 'status': 'Accepted'}
    ]
    
    # Mock file writing
    mock_open = mocker.patch('builtins.open', mocker.mock_open())
    
    # Call the main function
    result = scrape_and_clean.main(db_session)
    
    # Assert the function returned the correct count
    assert result == 1
    
    # Verify the scrape function was called with correct parameters
    mock_scrape.assert_called_once()
    
    # Verify the file was written
    mock_open.assert_called_with('new_structured_entries.json', 'w', encoding='utf-8')


@pytest.mark.db
def test_scrape_with_robots_txt_disallow(db_session, mocker):
    """Test that scraping respects robots.txt when disallowed.
    
    This test verifies that when robots.txt disallows scraping,
    the main function returns 0 and doesn't proceed with scraping.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock the robot parser to disallow scraping
    mock_robot_parser = mocker.patch('src.scrape_and_clean.RobotFileParser')
    mock_rp_instance = MagicMock()
    mock_rp_instance.can_fetch.return_value = False
    mock_robot_parser.return_value = mock_rp_instance
    
    # Mock the scrape function to verify it's not called
    mock_scrape = mocker.patch('src.scrape_and_clean.scrape_and_clean')
    
    # Call the main function
    result = scrape_and_clean.main(db_session)
    
    # Assert the function returned 0
    assert result == 0
    
    # Verify the scrape function was NOT called
    mock_scrape.assert_not_called()


@pytest.mark.db
def test_process_table_row():
    """Test the process_table_row function.
    
    This test verifies that the function correctly extracts
    entry data from HTML table rows.
    """
    from bs4 import BeautifulSoup
    from src.scrape_and_clean import process_table_row
    
    # Create mock HTML structure
    html = """
    <tr>
        <td>MIT</td>
        <td><span>Computer Science</span><span>PhD</span></td>
        <td>01 Sep</td>
        <td>Accepted on 15 Mar</td>
        <td><a href="/result/12345">Link</a></td>
    </tr>
    """
    soup = BeautifulSoup(html, 'html.parser')
    row = soup.find('tr')
    cells = row.find_all('td')
    
    # Create a mock detail row
    detail_html = """
    <tr>
        <td>
            <div class="tw-inline-flex">GPA: 3.9</div>
            <div class="tw-inline-flex">GRE: 320</div>
            <div class="tw-inline-flex">International</div>
            <div class="tw-inline-flex">Fall 2025</div>
        </td>
    </tr>
    """
    detail_soup = BeautifulSoup(detail_html, 'html.parser')
    detail_row = detail_soup.find('tr')
    
    # Mock the find_next_sibling method
    row.find_next_sibling = lambda tag: detail_row if tag == 'tr' else None
    
    # Process the row
    result = process_table_row(
        row, cells, 12345, 
        'https://www.thegradcafe.com/result/12345', 
        '2025-09-01'
    )
    
    # Verify the result
    assert result['pid'] == 12345
    assert result['university'] == 'MIT'
    assert result['program'] == 'Computer Science'
    assert result['degree'] == 'PhD'
    assert result['status'] == 'Accepted'
    assert result['date_added'] == '2025-09-01'
    assert result['url'] == 'https://www.thegradcafe.com/result/12345'


@pytest.mark.db
def test_flask_PIPELINE_IN_PROGRESS_flag(client, mocker):
    """Test that the pipeline flag prevents concurrent executions.
    
    This test verifies that the PIPELINE_IN_PROGRESS flag correctly
    prevents multiple concurrent pipeline executions.
    
    :param client: Flask test client fixture for making HTTP requests.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    import src.app as app_module
    
    # Set the flag to True to simulate an in-progress pipeline
    app_module.PIPELINE_IN_PROGRESS = True
    
    try:
        # Try to start another pipeline
        response = client.post('/pull-data')
        
        # Should get a 409 Conflict response
        assert response.status_code == 409
        assert "already in progress" in response.json['message']
    finally:
        # Reset the flag for other tests
        app_module.PIPELINE_IN_PROGRESS = False


@pytest.mark.db
def test_status_endpoint(client):
    """Test the /status endpoint returns correct pipeline status.
    
    This test verifies that the status endpoint correctly reports
    whether a pipeline is in progress.
    
    :param client: Flask test client fixture for making HTTP requests.
    :type client: flask.testing.FlaskClient
    """
    import src.app as app_module
    
    # Test with pipeline not in progress
    app_module.PIPELINE_IN_PROGRESS = False
    response = client.get('/status')
    assert response.status_code == 200
    assert response.json['PIPELINE_IN_PROGRESS'] is False
    
    # Test with pipeline in progress
    app_module.PIPELINE_IN_PROGRESS = True
    response = client.get('/status')
    assert response.status_code == 200
    assert response.json['PIPELINE_IN_PROGRESS'] is True
    
    # Reset for other tests
    app_module.PIPELINE_IN_PROGRESS = False

@pytest.mark.db
def test_pipeline_file_not_found_error(mocker, capsys):
    """Test the pipeline's error handling for FileNotFoundError.
    
    This test simulates a FileNotFoundError during the pipeline's execution
    to cover lines 76 in app.py. It verifies that the exception is caught
    and an appropriate error message is logged to standard output.
    
    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: The pytest fixture for capturing stdout and stderr.
    :type capsys: _pytest.capture.CaptureFixture
    """
    import src.app as app_module
    
    # Mock psycopg.connect to work normally
    mock_conn = mocker.MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mocker.patch('psycopg.connect', return_value=mock_conn)
    
    # Mock run_scrape_and_clean to raise FileNotFoundError
    mocker.patch('src.app.run_scrape_and_clean', 
                 side_effect=FileNotFoundError("Test file not found"))
    
    # Run the pipeline
    app_module.run_full_pipeline()
    
    # Check that the error was printed
    captured = capsys.readouterr()
    assert "File not found error in the pipeline: Test file not found" in captured.out
    assert "PIPELINE PROCESS HAS CONCLUDED" in captured.out

@pytest.mark.db
def test_pipeline_psycopg_error(mocker, capsys):
    """Test the pipeline's error handling for psycopg.Error.
    
    This test simulates a psycopg.Error during the pipeline's execution
    to cover line 74 in app.py. It verifies that database errors are
    properly caught and logged.
    
    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: The pytest fixture for capturing stdout and stderr.
    :type capsys: _pytest.capture.CaptureFixture
    """
    import src.app as app_module
    
    # Mock psycopg.connect to raise a psycopg.Error
    mocker.patch('psycopg.connect', 
                 side_effect=psycopg.Error("Database connection failed"))
    
    # Run the pipeline
    app_module.run_full_pipeline()
    
    # Check that the error was printed
    captured = capsys.readouterr()
    assert "Database error occurred in the pipeline:" in captured.out
    assert "PIPELINE PROCESS HAS CONCLUDED" in captured.out